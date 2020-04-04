#!/usr/bin/env python

import argparse
import gc
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from functools import partial

from dateutil.parser import parse

from fitbit import Fitbit
from influxdb import InfluxDBClient

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
    'distances',
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

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def try_cast_to_int(in_var):
    try:
        return int(in_var)
    except:
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


def create_api_datapoint(measurement, time, field_val, tag="data_dump"):
    if not field_val:
        field_val = 0.0
    return {
        "measurement": measurement,
        "tags": {
            "imported_from": "API"
        },
        "time": time,
        "fields": {'value': field_val}
    }


def get_last_timestamp_for_measurement(ifx_c, key_name, min_ts=0):
    res = ifx_c.query(
        'SELECT last(*) FROM {} ORDER BY time DESC LIMIT 1;'.format(key_name))
    if res:
        return parse(list(res.get_points(key_name))[0]['time'], ignoretz=True)
    return datetime.fromtimestamp(min_ts)


def get_period_for_measurement(ifx_c, key_name):
    last_record_ts = get_last_timestamp_for_measurement(ifx_c, key_name)
    if not last_record_ts:
        return 'max'
    cur_time = datetime.utcnow()
    t_delta = (cur_time - last_record_ts) / timedelta(days=1)
    if t_delta >= 7:
        return 30
    elif t_delta >= 30:
        return 90
    elif t_delta >= 90:
        return 180
    elif t_delta >= 180:
        return 365
    elif t_delta >= 365:
        return 'inf'
    return 'default'


def write_activities(ifx_c, act_list):
    act_list = [x['summary'] for x in act_list
                if x and x['summary']]
    datapoints = []
    for one_act in act_list:
        new_dict = {}
        for one_key in ACTIVITY_KEYS:
            if one_key in one_act:
                new_dict[one_key] = one_act[one_key]
        if 'distances' in one_act:
            for dist in one_act['distances']:
                new_dict['distance-{}'.format(dist['activity'])
                         ] = dist['distance']
        for key, value in new_dict.items():
            datapoints.append(create_api_datapoint(
                key, one_act['dateTime'], value))
    logger.debug('Going to write datapoints: %s', datapoints)
    ifx_c.write_points(datapoints, time_precision='s')


def save_var(folder, fname, value):
    with open(os.path.join(folder, fname), 'w') as out_f:
        out_f.write(value)


def load_var(folder, fname):
    with open(os.path.join(folder, fname), 'r') as in_f:
        return in_f.read()


def write_updated_credentials(cfg_path, new_info):
    save_var(cfg_path, 'access_token', new_info['access_token'])
    save_var(cfg_path, 'refresh_token', new_info['refresh_token'])
    save_var(cfg_path, 'expires_at', str(new_info['expires_in']))


def run_api_poller():
    cfg_path = try_getenv('CONFIG_PATH')
    db_host = try_getenv('DB_HOST')
    db_port = try_getenv('DB_PORT')
    db_user = try_getenv('DB_USER')
    db_password = try_getenv('DB_PASSWORD')
    db_name = try_getenv('DB_NAME')
    client_id = None
    client_secret = None
    access_token = None
    refresh_token = None

    redirect_url = try_getenv('CALLBACK_URL')
    expires_at = try_getenv('EXPIRES_AT')
    units = try_getenv('UNITS', 'it_IT')

    if os.path.isfile(os.path.join(cfg_path, 'client_id')):
        client_id = load_var(cfg_path, 'client_id')
    else:
        client_id = try_getenv('CLIENT_ID')
        save_var(cfg_path, 'client_id', client_id)

    if os.path.isfile(os.path.join(cfg_path, 'client_secret')):
        client_secret = load_var(cfg_path, 'client_secret')
    else:
        client_secret = try_getenv('CLIENT_SECRET')
        save_var(cfg_path, 'client_secret', client_secret)

    if os.path.isfile(os.path.join(cfg_path, 'access_token')):
        access_token = load_var(cfg_path, 'access_token')
    else:
        access_token = try_getenv('ACCESS_TOKEN')
        save_var(cfg_path, 'access_token', access_token)

    if os.path.isfile(os.path.join(cfg_path, 'refresh_token')):
        refresh_token = load_var(cfg_path, 'refresh_token')
    else:
        refresh_token = try_getenv('REFRESH_TOKEN')
        save_var(cfg_path, 'refresh_token', refresh_token)

    if os.path.isfile(os.path.join(cfg_path, 'expires_at')):
        expires_at = try_cast_to_int(load_var(cfg_path, 'expires_at'))
    else:
        expires_at = try_cast_to_int(try_getenv('EXPIRES_AT'))
        save_var(cfg_path, 'expires_at', (expires_at))

    logger.debug("client_id: %s, client_secret: %s, access_token: %s, refresh_token: %s",
                 client_id, client_secret, access_token, refresh_token)

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
    member_since = user_profile.get(
        'user', {}).get('memberSince', '1970-01-01')
    member_since_ts = parse(member_since, ignoretz=True).timestamp()
    logger.info('User is member since: %s (ts: %s)',
                member_since, member_since_ts)
    while True:
        call_count = 0
        # Get an influxDB client connection
        db_client = InfluxDBClient(
            db_host, db_port, db_user, db_password, db_name)
        db_client.create_database(db_name)

        timestamps = []
        for act in ACTIVITIES:
            timestamps.append(
                get_last_timestamp_for_measurement(db_client, act, member_since_ts))
        ts_max = max(timestamps)
        cur_ts = ts_max

        all_activities = []
        while cur_ts <= datetime.utcnow():
            res_dict = api_client.activities(date=cur_ts)
            res_dict['summary']['dateTime'] = cur_ts
            all_activities.append(res_dict)
            call_count += 1
            cur_ts += timedelta(days=1)
            if call_count > 1:
                break
        write_activities(db_client, all_activities)
        del all_activities

        if call_count > 1:
            time.sleep(60*60+3)
            db_client.close()
            del db_client
            gc.collect()
            continue

        db_client.close()
        del db_client
        gc.collect()
        time.sleep(60*60+3)


if __name__ == "__main__":
    run_api_poller()
