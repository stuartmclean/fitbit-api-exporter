#!/usr/bin/env python3

from dateutil.parser import parse
from glob import glob
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os
import json

CHUNK_SIZE = 5000


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


def filter_estimated_oxygen(row):
    if 'Infrared' not in row:
        dt, val = row.split(',')
        return { 'dateTime': dt, 'value': val }
    return {}


def filter_weight(row):
    row['dateTime'] = row['date'] + ' ' + row['time']
    del row['date'], row['time']
    return row


def downcast(x):
    ret = x
    try:
        ret = float(x)
    except ValueError:
        return ret

    try:
        ret = int(x)
    except ValueError:
        return ret

    return ret

measurements = {
        'altitude': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'calories': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(float(x)*1000) } },
        'demographic_vo2_max': { 'time': 'dateTime', 'fields': { 'demographicVO2Max': lambda x: float(x), 'demographicVO2MaxError': lambda x: float(x), 'filteredDemographicVO2Max': lambda x: float(x), 'filteredDemographicVO2MaxError': lambda x: float(x) } },
        'distance': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'estimated_oxygen_variation': { 'extract': lambda x: filter_estimated_oxygen(x), 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'heart_rate': { 'time': 'dateTime', 'fields': { 'bpm': lambda x: int(x), 'confidence': lambda x: int(x) } },
        'lightly_active_minutes': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'moderately_active_minutes': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'resting_heart_rate': { 'time': 'dateTime', 'fields': { 'value': lambda x: float(x), 'error': lambda x: float(x) } },
        'run_vo2_max': { 'time': 'dateTime', 'fields': { 'runVO2Max': lambda x: float(x), 'runVO2MaxError': lambda x: float(x), 'filteredRunVO2Max': lambda x: float(x), 'filteredRunVO2MaxError': lambda x: float(x) } },
        'sedentary_minutes': { 'time': 'dateTime', 'fields': { 'value': lambda x: float(x) } },
        'swim_lengths_data': { 'time': 'dateTime', 'fields': { 'lapDurationSec': lambda x: int(x), 'strokeCount': lambda x: int(x) } },
        'very_active_minutes': { 'time': 'dateTime', 'fields': { 'value': lambda x: int(x) } },
        'weight': { 'extract': lambda x: filter_weight(x), 'time': 'dateTime', 'fields': { 'bmi': lambda x: float(x), 'fat': lambda x: float(x), 'weight': lambda x: float(x)*0.45359 } }
        }


def merge_files(flist):
    merged_list = []
    for onef in flist:
        with open(onef, 'r') as inf:
            js = json.load(inf)
            if isinstance(js, list):
                merged_list.extend(js)
            else:
                merged_list.append(js)
    return merged_list


def create_datapoint(measurement, time, fields, tag="data_dump"):
    return { "measurement": measurement, "tags": { "imported_from": tag }, "time": time, "fields": fields }


def dedup_meas(list_of_meas):
    tracking_set = set()
    new_list = []
    for record in list_of_meas:
        if record['time'] not in tracking_set:
            tracking_set.add(record['time'])
            new_list.append(record)
        else:
            print("Duplicated record: {}".format(record))
    return new_list


def write_data_for(dump_folder, keyname, influx_client):
    print('---------')
    print("Key: {}".format(keyname))
    last_ts = None
    last_ts = influx_client.query('SELECT * FROM {} ORDER BY time DESC LIMIT 1;'.format(keyname))
    if not last_ts:
        print("Could not get timestamp of last value")
    else:
        last_ts = parse(list(last_ts.get_points(keyname))[0]['time'], ignoretz=True)
    print("Last timestamp: {}".format(last_ts))
    unproc_list = merge_files(sorted(glob(os.path.join(dump_folder, keyname) + '-*.json')))
    if 'extract' in measurements[keyname]:
        unproc_list = [measurements[keyname]['extract'](x) for x in unproc_list]
    final_list = []
    for oneitem in unproc_list:
        idict = oneitem
        if 'value' in oneitem:
            if isinstance(oneitem['value'], dict):
                idict = oneitem['value']
        ndict = {}
        for k, v in measurements[keyname]['fields'].items():
            if k in idict:
                ndict[k] = v(idict[k])
            else:
                ndict[k] = v(0)
        final_list.append(create_datapoint(keyname, oneitem[measurements[keyname]['time']], ndict))
    final_list = dedup_meas(final_list)
    print("Done reading files for key: {}, {:,} items".format(keyname, len(final_list)))
    if last_ts:
        print("Filtering list via timestamp...")
        final_list = [x for x in final_list if parse(x['time'], ignoretz=True) >= last_ts]
        print("Remaining values for writing: {}".format(len(final_list)))
        if not final_list:
            print("All values were already written in a previous iteration, returning without writes")
            return
    if keyname in [x['name'] for x in influx_client.get_list_measurements()]:
        first_field = list(measurements[keyname]['fields'].keys())[0]
        print('Checking {}.{}'.format(keyname, first_field))
        key_count = influx_client.query('SELECT COUNT({}) FROM {};'.format(first_field, keyname))
        print("ResultSet key_count: {}".format(key_count))
        key_count = list(key_count.get_points(keyname))[0]['count']
        print('key_count is: {:,}'.format(key_count))
        if key_count == len(final_list):
            print("Key already fully written to DB, skipping")
            return
    del unproc_list
    print("Writing to database key: {} with {:,} items".format(keyname, len(final_list)))
    while len(final_list) > 0:
        influx_client.write_points(final_list[:CHUNK_SIZE])
        final_list = final_list[CHUNK_SIZE:]
    print("Done writing to DB for key: {}".format(keyname))


def mainfunc():
    load_dotenv()
    db_host = try_getenv('DB_HOST')
    db_port = try_getenv('DB_PORT')
    db_user = try_getenv('DB_USER')
    db_password = try_getenv('DB_PASSWORD', None)
    db_name = try_getenv('DB_NAME', None)

    required_folder = os.path.join('/dump', 'user-site-export')
    if not os.path.isdir(required_folder):
        print("Folder does not contain 'user-site-export' folder: {}".format(required_folder))
        raise ValueError

    client = InfluxDBClient(host=db_host, port=db_port, username=db_user, password=db_password, database=db_name)
    client.create_database(db_name)
    for k in sorted(measurements):
        write_data_for(required_folder, k, client)
    print('All done!')


if __name__ == "__main__":
    mainfunc()
