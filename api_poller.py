#!/usr/bin/env python

import argparse
import gc
import logging
import os
import time

from datetime import datetime, timedelta
from dateutil.parser import parse
from fitbit import Fitbit
from influxdb import InfluxDBClient

ACTIVITIES=[
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

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def try_getenv(var_name):
    my_var = os.environ(var_name)
    try:
        my_var = int(my_var)
    except Exception:
        pass
    if not my_var:
        errstr = 'Invalid or missing value provided for: {}'.format(var_name)
        raise ValueError(errstr)
    return my_var

def create_api_datapoint(measurement, time, fields, tag="data_dump"):
    return {
            "measurement": measurement,
            "tags": {
                "imported_from": "API"
                },
            "time": time,
            "fields": fields
            }

def get_last_timestamp_for_measurement(ifx_c, key_name):
    res = ifx_c.query('SELECT last(*) FROM {} ORDER BY time DESC LIMIT 1;')
    if res:
        return parse(list(res.get_points(key_name))[0]['time'], ignoretz=True)
    return datetime.fromtimestamp(0)

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
    for one_act in act_list:
        # TODO
        pass

def run_api_poller():
    db_host = try_getenv('DB_HOST')
    db_port = try_getenv('DB_PORT')
    db_user = try_getenv('DB_USER')
    db_password = try_getenv('DB_PASSWORD')
    db_name = try_getenv('DB_NAME')
    client_id = try_getenv('CLIENT_ID')
    client_secret = try_getenv('CLIENT_SECRET')
    access_token = try_getenv('ACCESS_TOKEN')
    refresh_token = try_getenv('REFRESH_TOKEN')
    redirect_url = try_getenv('CALLBACK_URL')
    code = try_getenv('CODE')
    units = try_getenv('UNITS', 'it_IT')

    logger.debug("client_id: %s, client_secret: %s, access_token: %s, refresh_token: %s", client_id, client_secret, access_token, refresh_token)

    api_client = Fitbit(
            client_id=client_id,
            client_secret=client_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            redirect_uri=redirect_url,
            system=units
            )
    while True:
        call_count = 0
        # Get an influxDB client connection
        db_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)
        db_client.create_database(db_name)

        timestamps = []
        for act in ACTIVITIES:
            timestamps.append(get_last_timestamp_for_measurement(db_client, act))
        ts_max = max(timestamps)
        cur_ts = ts_max

        all_activities = []
        while cur_ts <= datetime.utcnow():
            all_activities.append(api_client.activities(date=cur_ts))
            call_count += 1
            cur_ts += timedelta(days=1)
            if call_count >= 150:
                break

        write_activities(db_client, all_activities)
        if call_count > = 150:
            time.sleep(60*60+3)
            continue
############ FATTO FINO A QUI
        db_client.close()
        del db_client
        gc.collect()
        time.sleep(60*60+3)


if __name__ == "__main__":
    run_api_poller()
