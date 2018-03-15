#!/usr/bin/env python3
"""
bleeplog.py

A simple wrapper around rtl_fm and multimon-ng that parses POCSAG pager/bleep messages and forwards them to AWS DynamoDB.
"""

import subprocess
from datetime import datetime, timezone
import requests
import uuid
import logging


# Apparently multimon is happy without squelch (-l 50)
command = "rtl_fm -f 454022700 -M fm -s 22050 -g 50 -E dc -E deemp -F 0 -p 20 | multimon-ng -a POCSAG512 -f alpha -t raw -"
pushover_api_key = ""
pushover_api_user = ""
pushover_api_url = "https://api.pushover.net/1/messages.json"
aws_gateway_api_url = ""
aws_gateway_api_key = ""
proxies = {
    'http': '',
    'https': ''
}
personal_bleeps = [] # Bleep addresses that we want notifications for
crash_bleep = '' # Bleep address of crash bleep
tel_prefix = '' # Telephone prefix before extension for direct dial number

with requests.Session() as s:
    s.headers.update({'x-api-key': aws_gateway_api_key})
    s.proxies.update(proxies)

    with subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True, shell=True) as process:
        for line in process.stdout:
            if line.startswith('POCSAG') and 'Alpha:' in line:
                # POCSAG512: Address: 2000565  Function: 0  Alpha:   4012<EOT>
                # POCSAG512: Address: 2000356  Function: 0  Alpha:   Please call SWBD on 3003 13-Mar-2018 23:38:52
                bleepId = uuid.uuid4()
                timestamp = datetime.utcnow().isoformat()
                address = line[11:27].split(': ')[1]
                function = line[29:40].split(': ')[1]
                message = line[42:].split(':   ')[1].rstrip('<EOT>\n')

                # Build payload for DynamoDB item
                dynamo_item = {
                    "Item": {
                        "BleepId": {"S": "{}".format(bleepId)},
                        "Timestamp": {"S": "{}".format(timestamp)},
                        "Address": {"S": "{}".format(address)},
                        "Message": {"S": "{}".format(message)}
                        },
                    "TableName": "Bleeps"
                }

                try:
                    r = s.post(log_api_url, json=dynamo_item)
                except requests.exceptions.RequestException as e:
                    logging.error('Error posting to AWS Lamdbda: {}'.format(e))
                
                # Notify of personal bleeps
                if address in personal_bleeps:
                    payload = {
                        "token": pushover_api_key,
                        "user": pushover_api_user,
                        "message": "Bleep {}: {}".format(address, message)
                        }

                    # Create a direct dial link if the message looks like a 4-digit extension
                    if message.isdigit() and len(message) == 4:
                        payload['url'] = "tel://{}{}".format(tel_prefix, message)
                        payload['url_title'] = "Call x{}".format(message)
                    
                    # Set an emergency priority if it's a crash bleep
                    if address == crash_bleep:
                        payload['priority'] = 2
                        payload['retry'] = 30
                        payload['expire'] = 300
                        payload['message'] = "Cardiac Arrest: {}".format(message)

                    try:
                        r = requests.post(pushover_api_url, json=payload, proxies=proxies)
                    except requests.exceptions.RequestException as e:
                        logging.error('Error pushing to Pushover: {}'.format(e))
                
                logging.debug("Logged bleep: {} - {}".format(address, message))

