#!/usr/bin/env python

# pylint: disable=missing-docstring

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from functools import partial

from dateutil.parser import parse
from fitbit import Fitbit
from fitbit.exceptions import HTTPServerError, HTTPTooManyRequests, Timeout
from influxdb import InfluxDBClient

API_SUPPORTED_QUERIES = [
    'activities',
    'body',
    'bp',
    'foods_log',
    'foods_log_water',
    'glucose'
    'heart',
    'sleep'
]

TIME_SERIES = {
    'activities': [
        'activityCalories',
        'calories',
        'caloriesBMR',
        'distance',
        'elevation',
        'floors',
        'minutesFairlyActive',
        'minutesLightlyActive',
        'minutesSedentary',
        'minutesVeryActive',
        'steps'
    ],
    'activities_tracker': [
        'activityCalories',
        'calories',
        'distance',
        'elevation',
        'floors',
        'minutesFairlyActive',
        'minutesLightlyActive',
        'minutesSedentary',
        'minutesVeryActive',
        'steps'
    ]
}

ACTIVITIES = [
    'activityCalories',
    'caloriesBMR',
    'caloriesOut',
    'elevation',
    'fairlyActiveMinutes',
    'floors',
    'lightlyActiveMinutes',
    'marginalCalories',
    'sedentaryMinutes',
    'steps',
    'veryActiveMinutes'
]

ACTIVITY_KEYS = [
    'activityCalories',
    'caloriesBMR',
    'caloriesOut',
    'elevation',
    'fairlyActiveMinutes',
    'floors',
    'lightlyActiveMinutes',
    'marginalCalories',
    'sedentaryMinutes',
    'steps',
    'veryActiveMinutes'
]

PERIOD_MAPS = {
    1: '1d',
    30: '3m',
    90: '6m',
    180: '1y',
    365: 'max',
    9999: 'max'
}

REQUEST_INTERVAL = timedelta(days=127)

logging.basicConfig()
logger = logging.getLogger()  # pylint: disable=invalid-name
logger.setLevel(logging.DEBUG)


def try_cast_to_int(in_var):
    try:
        return int(in_var)
    except Exception:
        return in_var


def try_getenv(var_name, default_var=None):
    my_var = os.environ.get(var_name)
    my_var = try_cast_to_int(my_var)
    if not my_var:
        if default_var:
            return default_var
        errstr = 'Invalid or missing value provided for: {}'.format(var_name)
        raise ValueError(errstr)
    return my_var


def create_api_datapoint_meas_series(measurement, series, value, in_dt):
    if not value:
        value = 0.0
    try:
        value = float(value)
    except Exception:
        pass
    return {
        "measurement": measurement,
        "tags": {"imported_from": "API"},
        "time": in_dt,
        "fields": {series: value}
    }


def get_last_timestamp_for_measurement(ifx_c, meas, series, min_ts=0):
    res = ifx_c.query('SELECT last({}) FROM {};'.format(series, meas))
    logger.debug('get_last: res: %s', res)
    if res:
        return parse(list(res.get_points())[0]['time'], ignoretz=True)
    return min_ts


def get_first_timestamp_for_measurement(ifx_c, meas, series, min_ts=0):
    res = ifx_c.query('SELECT first({}) FROM {};'.format(series, meas))
    logger.debug('get_first: res: %s', res)
    if res:
        return parse(list(res.get_points())[0]['time'], ignoretz=True)
    return min_ts


def save_var(folder, fname, value):
    with open(os.path.join(folder, fname), 'w') as out_f:
        out_f.write(value)


def load_var(folder, fname):
    with open(os.path.join(folder, fname), 'r') as in_f:
        return in_f.read()


def try_load_var(folder, fname):
    if os.path.isfile(os.path.join(folder, fname)):
        return load_var(folder, fname)
    return None


def write_updated_credentials(cfg_path, new_info):
    save_var(cfg_path, 'access_token', new_info['access_token'])
    save_var(cfg_path, 'refresh_token', new_info['refresh_token'])
    save_var(cfg_path, 'expires_at', str(new_info['expires_in']))


def append_between_day_series(in_list, cur_marker, interval_max):
    while cur_marker <= interval_max:
        if cur_marker + REQUEST_INTERVAL > interval_max:
            in_list.append((cur_marker, interval_max))
            break
        else:
            in_list.append((cur_marker, cur_marker + REQUEST_INTERVAL))
            cur_marker += REQUEST_INTERVAL + timedelta(days=1)


def run_api_poller():
    cfg_path = try_getenv('CONFIG_PATH')
    earliest_day_requested = try_load_var(cfg_path, 'earliest_day_requested')
    db_host = try_getenv('DB_HOST')
    db_port = try_getenv('DB_PORT')
    db_user = try_getenv('DB_USER')
    db_password = try_getenv('DB_PASSWORD')
    db_name = try_getenv('DB_NAME')
    redirect_url = try_getenv('CALLBACK_URL')
    units = try_getenv('UNITS', 'it_IT')

    # These are required vars, that we first  try to load from file
    client_id = try_load_var(cfg_path, 'client_id')
    client_secret = try_load_var(cfg_path, 'client_secret')
    access_token = try_load_var(cfg_path, 'access_token')
    refresh_token = try_load_var(cfg_path, 'refresh_token')
    expires_at = try_load_var(cfg_path, 'expires_at')

    # If any of the required vars is not in file, try to read from env
    # If read, save
    if not client_id:
        client_id = try_getenv('CLIENT_ID')
        save_var(cfg_path, 'client_id', client_id)
    if not client_secret:
        client_secret = try_getenv('CLIENT_SECRET')
        save_var(cfg_path, 'client_secret', client_secret)
    if not access_token:
        access_token = try_getenv('ACCESS_TOKEN')
        save_var(cfg_path, 'access_token', access_token)
    if not refresh_token:
        refresh_token = try_getenv('REFRESH_TOKEN')
        save_var(cfg_path, 'refresh_token', refresh_token)
    if not expires_at:
        expires_at = try_cast_to_int(try_getenv('EXPIRES_AT'))
        save_var(cfg_path, 'expires_at', str(expires_at))

    logger.debug("client_id: %s, client_secret: %s, access_token: %s, refresh_token: %s, expires_at: %s",
                 client_id, client_secret, access_token, refresh_token, expires_at)

    if not client_id:
        logging.critical("client_id missing, aborting!")
        sys.exit(1)
    if not client_secret:
        logging.critical("client_secret missing, aborting!")
        sys.exit(1)
    if not access_token:
        logging.critical("access_token missing, aborting!")
        sys.exit(1)
    if not refresh_token:
        logging.critical("refresh_token missing, aborting!")
        sys.exit(1)

    api_client = Fitbit(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        redirect_uri=redirect_url,
        refresh_cb=partial(write_updated_credentials, cfg_path),
        system=Fitbit.METRIC
    )

    user_profile = api_client.user_profile_get()
    api_call_count = 1

    member_since = user_profile.get('user', {}).get('memberSince', '1970-01-01')
    member_since_dt = parse(member_since, ignoretz=True)
    member_since_ts = parse(member_since, ignoretz=True).timestamp()
    logger.info('User is member since: %s (ts: %s)', member_since, member_since_ts)

    cur_day = datetime.utcnow()
    day_to_request = cur_day
    #next_sleep = (60*60+3)
    next_sleep = 1

    # Get an influxDB client connection
    db_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
    db_client.create_database(db_name)

    # First try to fill any gaps: between User_member_since and first_ts,
    # and then between last_ts and cur_day
    to_fetch = {}
    for meas, series_list in TIME_SERIES.items():
        for series in series_list:
            resource = '{}/{}'.format(meas, series)
            if '_' in meas:
                resource = resource.replace('_', '/', 1)
            first_ts = get_first_timestamp_for_measurement(db_client, meas, series, min_ts=cur_day)
            last_ts = get_last_timestamp_for_measurement(db_client, meas, series, min_ts=cur_day)
            profile_to_first = int((first_ts - member_since_dt)/timedelta(days=1))
            last_to_current = int((cur_day - last_ts)/timedelta(days=1))
            logger.debug('first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                         first_ts, last_ts, profile_to_first, last_to_current)

            intervals_to_fetch = []
            if profile_to_first > 1:
                append_between_day_series(intervals_to_fetch, member_since_dt, first_ts)
            if last_to_current > 1:
                append_between_day_series(intervals_to_fetch, last_ts, cur_day)

            for one_tuple in intervals_to_fetch:
                results = None
                while True:
                    try:
                        results = api_client.time_series(resource, base_date=one_tuple[0], end_date=one_tuple[1])
                        break
                    except Timeout as ex:
                        logger.warning('Request timed out, retrying in 15 seconds...')
                        time.sleep(15)
                    except HTTPServerError as ex:
                        logger.warning('Server returned exception (5xx), retrying in 3 seconds (%s)', ex)
                        time.sleep(3)
                    except HTTPTooManyRequests as ex:
                        # 150 API calls done, and python-fitbit doesn't provide the retry-after header, so stop trying
                        # and allow the limit to reset, even if it costs us one hour
                        logger.info('API limit reached, sleeping for 3610 seconds!')
                        time.sleep(3601)
                    except Exception as ex:
                        logger.exception('Got some unexpected exception')
                        raise
                if not results:
                    logger.error('Error trying to fetch results, bailing out')
                    sys.exit(4)
                datapoints = []
                logger.debug('full_request: %s', results)
                for one_d in list(results.values())[0]:
                    logger.debug('Creating datapoint for %s, %s, %s', meas, series, one_d)
                    datapoints.append(create_api_datapoint_meas_series(
                        meas, series, one_d.get('value'), one_d.get('dateTime')))
                # TODO Delete last_ts before writing to db, might have been a partial write
                db_client.write_points(datapoints, time_precision='h')
    db_client.close()
    del db_client

    sys.exit(0)


if __name__ == "__main__":
    run_api_poller()
