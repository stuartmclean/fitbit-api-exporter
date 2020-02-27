#!/usr/bin/env python

import argparse
import os
import time

from fitbit import Fitbit
from influxdb import InfluxDBClient

def create_api_datapoint(measurement, time, fields, tag="data_dump"):
    return {
            "measurement": measurement,
            "tags": {
                "imported_from": "API"
                },
            "time": time,
            "fields": fields
            }

def run_api_poller():
    os_env = os.environ

    client_id = os_env.get('CLIENT_ID', '')
    client_secret = os_env.get('CLIENT_SECRET', '')
    access_token = os_env.get('ACCESS_TOKEN', '')
    redirect_url = os_env.get('CALLBACK_URL', '')
    code = os_env.get('CODE', '')
    units = os_env.get('UNITS', '')

    if (not client_id and not client_secret) or not access_token:
        raise Exception('No auth info given!')

    client = Fitbit(client_id, client_secret, redirect_uri=redirect_url, system=units)
    while True:
        # Get an influxDB client connection
        client = InfluxDBClient('localhost', 8086, 'root', 'root', 'fitbit')
        client.create_database('fitbit')

        for point in client.body():
            point['dt'] = point['date'] + ' ' + point['time']
            client.write_points([create_datapoint('fat', point['dt'], float(point['fat']))])

        for point in client.activities():


        time.sleep(60*60)


if __name__ == "__main__":
    run_api_poller()
