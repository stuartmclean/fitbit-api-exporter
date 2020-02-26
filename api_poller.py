#!/usr/bin/env python

import argparse
import os

def run_api_poller():
    os_env = os.environ

    client_id = os_env.get('CLIENT_ID', '')
    client_secret = os_env.get('CLIENT_SECRET', '')
    access_token = os_env.get('ACCESS_TOKEN', '')

    if (not client_id and not client_secret) or not access_token:
        raise Exception('No auth info given!')


if __name__ == "__main__":
    run_api_poller()
