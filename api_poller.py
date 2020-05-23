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


def transform_body_log_fat_datapoint(datapoint):
    ret_dps = [{
        'dateTime': datetime.fromtimestamp(int(datapoint['logId'])/1000),
        'meas': 'body_log',
        'series': 'fat_fat',
        'value': datapoint.get('fat', 0.0)
    }]
    logger.debug('Returning body_log_fat datapoints: %s', ret_dps)
    return ret_dps


def transform_body_log_weight_datapoint(datapoint):
    ret_dps = [
        {
            'dateTime': datetime.fromtimestamp(int(datapoint['logId'])/1000),
            'meas': 'body_log',
            'series': 'weight_bmi',
            'value': datapoint.get('bmi', 0.0)
        },
        {
            'dateTime': datetime.fromtimestamp(int(datapoint['logId'])/1000),
            'meas': 'body_log',
            'series': 'weight_fat',
            'value': datapoint.get('fat', 0.0)
        },
        {
            'dateTime': datetime.fromtimestamp(int(datapoint['logId'])/1000),
            'meas': 'body_log',
            'series': 'weight_weight',
            'value': datapoint.get('weight', 0.0)
        }
    ]
    logger.debug('Returning body_log_weight datapoints: %s', ret_dps)
    return ret_dps


def transform_activities_heart_datapoint(datapoint):
    logger.debug('transform_activities_heart_datapoint: %s', datapoint)
    d_t = datapoint['dateTime']
    dp_value = datapoint['value']
    ret_dps = [
        {
            'dateTime': d_t,
            'meas': 'activities',
            'series': 'restingHeartRate',
            'value': dp_value.get('restingHeartRate', 0.0)
        }
    ]
    if dp_value.get('heartRateZones'):
        for zone in dp_value['heartRateZones']:
            for one_val in ['caloriesOut', 'max', 'min', 'minutes']:
                series_name = '_'.join(['hrz', zone['name'].replace(' ', '_').lower(), one_val])
                ret_dps.append({
                    'dateTime': d_t,
                    'meas': 'activities',
                    'series': series_name,
                    'value': zone.get(one_val, 0.0)
                })
    logger.debug('Returning activities_heart datapoints: %s', ret_dps)
    return ret_dps


def transform_sleep_datapoint(datapoint):
    d_t = datapoint['startTime']
    ret_dps = [
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'duration',
            'value': datapoint.get('duration', 0) / 1000
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'efficiency',
            'value': datapoint.get('efficiency')
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'isMainSleep',
            'value': datapoint.get('isMainSleep', False)
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'timeInBed',
            'value': datapoint.get('timeInBed')
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'minutesAfterWakeup',
            'value': datapoint.get('minutesAfterWakeup')
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'minutesAsleep',
            'value': datapoint.get('minutesAsleep')
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'minutesAwake',
            'value': datapoint.get('minutesAwake')
        },
        {
            'dateTime': d_t,
            'meas': 'sleep',
            'series': 'minutesToFallAsleep',
            'value': datapoint.get('minutesToFallAsleep')
        }
    ]
    if datapoint.get('levels'):
        if datapoint.get('summary'):
            for one_level, dict_level in datapoint['levels']['summary'].items():
                for one_val in ['count', 'minutes', 'thirtyDayAvgMinutes']:
                    ret_dps.append({
                        'dateTime': d_t,
                        'meas': 'sleep_levels',
                        'series': one_level.lower() + '_' + one_val,
                        'value': dict_level.get(one_val)
                    })
        if datapoint.get('data'):
            for data_entry in datapoint['levels']['data']:
                for one_val in ['level', 'seconds']:
                    ret_dps.append({
                        'dateTime': data_entry['datetime'],
                        'meas': 'sleep_data',
                        'series': 'level_' + data_entry['level'],
                        'value': data_entry['seconds']
                    })
        if datapoint.get('shortData'):
            for data_entry in datapoint['levels']['shortData']:
                for one_val in ['level', 'seconds']:
                    ret_dps.append({
                        'dateTime': data_entry['datetime'],
                        'meas': 'sleep_shortData',
                        'series': 'level_' + data_entry['level'],
                        'value': data_entry['seconds']
                    })
    logger.debug('Returning sleep datapoints: %s', ret_dps)
    return ret_dps


BASE_SERIES = {
    'activities': {
        'activityCalories': None,  # dateTime, value
        'calories': None,  # dateTime, value
        'caloriesBMR': None,  # dateTime, value
        'distance': None,  # dateTime, value
        'elevation': None,  # dateTime, value
        'floors': None,  # dateTime, value
        'heart': {
            # https://dev.fitbit.com/build/reference/web-api/heart-rate/
            'key_series': 'restingHeartRate',
            'transform': transform_activities_heart_datapoint
        },
        'minutesFairlyActive': None,  # dateTime, value
        'minutesLightlyActive': None,  # dateTime, value
        'minutesSedentary': None,  # dateTime, value
        'minutesVeryActive': None,  # dateTime, value
        'steps': None  # dateTime, value
    },
    'activities_tracker': [
        'activityCalories',  # dateTime, value
        'calories',  # dateTime, value
        'distance',  # dateTime, value
        'elevation',  # dateTime, value
        'floors',  # dateTime, value
        'minutesFairlyActive',  # dateTime, value
        'minutesLightlyActive',  # dateTime, value
        'minutesSedentary',  # dateTime, value
        'minutesVeryActive',  # dateTime, value
        'steps'  # dateTime, value
    ],
    'body': [
        'bmi',  # dateTime, value
        'fat',  # dateTime, value
        'weight'  # dateTime, value
    ],
    'body_log': {
        'fat': {
            'key_series': 'fat_fat',
            # date, fat, logId, source: 'API', time
            'transform': transform_body_log_fat_datapoint
        },
        'weight': {
            'key_series': 'weight_weight',
            # bmi, date, fat, logId, source: 'API', time, weight
            'transform': transform_body_log_weight_datapoint
        }
    },
    'foods_log': [
        'caloriesIn',
        'water'
    ],
    'sleep': {
        'sleep': {
            'key_series': 'efficiency',
            # supercomplex type: https://dev.fitbit.com/build/reference/web-api/sleep/
            'transform': transform_sleep_datapoint
        }
    }
}


# Body series have max 31 days at a time, be a bit more conservative
REQUEST_INTERVAL = timedelta(days=27)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(lineno)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()  # pylint: disable=invalid-name


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


def fitbit_fetch_datapoints(api_client, meas, series, resource, intervals_to_fetch):
    datapoints = []
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
                logger.warning('Server returned exception (5xx), retrying in 15 seconds (%s)', ex)
                time.sleep(15)
            except HTTPTooManyRequests as ex:
                # 150 API calls done, and python-fitbit doesn't provide the retry-after header, so stop trying
                # and allow the limit to reset, even if it costs us one hour
                logger.info('API limit reached, sleeping for 3610 seconds!')
                time.sleep(3610)
            except Exception as ex:
                logger.exception('Got some unexpected exception')
                raise
        if not results:
            logger.error('Error trying to fetch results, bailing out')
            sys.exit(4)
        logger.debug('full_request: %s', results)
        for one_d in list(results.values())[0]:
            logger.debug('Creating datapoint for %s, %s, %s', meas, series, one_d)
            datapoints.append(one_d)
    return datapoints


def run_api_poller():
    cfg_path = try_getenv('CONFIG_PATH')
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

    user_profile = None
    while True:
        try:
            user_profile = api_client.user_profile_get()
            break
        except Timeout as ex:
            logger.warning('Request timed out, retrying in 15 seconds...')
            time.sleep(15)
        except HTTPServerError as ex:
            logger.warning('Server returned exception (5xx), retrying in 15 seconds (%s)', ex)
            time.sleep(15)
        except HTTPTooManyRequests as ex:
            # 150 API calls done, and python-fitbit doesn't provide the retry-after header, so stop trying
            # and allow the limit to reset, even if it costs us one hour
            logger.info('API limit reached, sleeping for 3610 seconds!')
            time.sleep(3610)
        except Exception as ex:
            logger.exception('Got some unexpected exception')
            raise

    member_since = user_profile.get('user', {}).get('memberSince', '1970-01-01')
    member_since_dt = parse(member_since, ignoretz=True)
    member_since_ts = parse(member_since, ignoretz=True).timestamp()
    logger.info('User is member since: %s (ts: %s)', member_since, member_since_ts)

    cur_day = datetime.utcnow()

    db_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
    for one_db in db_client.get_list_database():
        if one_db['name'] == db_name:
            break
    else:
        db_client.create_database(db_name)
    db_client.close()

    # First try to fill any gaps: between User_member_since and first_ts,
    # and then between last_ts and cur_day
    while True:
        for meas, series_list in BASE_SERIES.items():
            for series in series_list:
                resource = '{}/{}'.format(meas, series)
                if '_' in meas:
                    resource = resource.replace('_', '/', 1)
                if resource == 'sleep/sleep':
                    # Sleep is special, is its own main category
                    resource = 'sleep'

                db_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)

                key_series = series
                if isinstance(series_list, dict) and series_list.get(series):
                    # Datapoints are retrieved with all keys in the same dict, so makes no sense to retrieve individual
                    # series names. Use one series as the key series.
                    key_series = series_list[series]['key_series']

                first_ts = get_first_timestamp_for_measurement(db_client, meas, key_series, min_ts=cur_day)
                last_ts = get_last_timestamp_for_measurement(db_client, meas, key_series, min_ts=cur_day)
                profile_to_first = int((first_ts - member_since_dt)/timedelta(days=1))
                last_to_current = int((cur_day - last_ts)/timedelta(days=1))
                logger.debug('key_series: %s, first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                             key_series, first_ts, last_ts, profile_to_first, last_to_current)
                db_client.close()

                intervals_to_fetch = []
                if profile_to_first > 1:
                    append_between_day_series(intervals_to_fetch, member_since_dt, first_ts)
                if last_to_current > 1:
                    append_between_day_series(intervals_to_fetch, last_ts, cur_day)
                if not intervals_to_fetch:
                    logger.info('No gaps to fetch for %s, %s: fetching last day only', meas, series)
                    intervals_to_fetch.append((cur_day, cur_day,))

                # DB can't be open here, because fitbit_fetch_datapoints can hang for a long time
                if meas == 'sleep':
                    api_client.API_VERSION = '1.2'
                datapoints = fitbit_fetch_datapoints(api_client, meas, series, resource, intervals_to_fetch)
                if meas == 'sleep':
                    api_client.API_ENDPOINT = '1'
                converted_dps = []
                for one_d in datapoints:
                    if not one_d:
                        continue
                    if isinstance(series_list, dict) and series_list.get(series):
                        new_dps = series_list[series]['transform'](one_d)
                        for one_dd in new_dps:
                            converted_dps.append(create_api_datapoint_meas_series(
                                one_dd['meas'], one_dd['series'], one_dd['value'], one_dd['dateTime']
                            ))
                    else:
                        converted_dps.append(create_api_datapoint_meas_series(
                            meas, series, one_d.get('value'), one_d.get('dateTime')))

                db_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
                precision = 'h'
                if meas == 'sleep':
                    precision = 's'
                logger.debug('Going to write %s points, key_series: %s, first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                             len(converted_dps), key_series, first_ts, last_ts, profile_to_first, last_to_current)
                logger.debug('First 3: %s', converted_dps[:3])
                logger.debug('Last 3: %s', converted_dps[-3:])
                if not db_client.write_points(converted_dps, time_precision=precision, batch_size=2500):
                    logger.critical('key_series: %s, first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                                    key_series, first_ts, last_ts, profile_to_first, last_to_current)
                    raise Exception('Unable to write points!')
                db_client.close()
        logger.info('All series processed, sleeping for 4h')
        time.sleep(3610*4)

    sys.exit(0)


if __name__ == "__main__":
    run_api_poller()
