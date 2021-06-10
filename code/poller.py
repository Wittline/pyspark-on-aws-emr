import logging
import sys
import time
import boto3

def status_poller(intro, done_status, func):

    emr_client = boto3.client('emr')    
    status = None
    print(intro)
    print("Current status: ", end='')
    while status != done_status:
        prev_status = status
        status = func()
        if prev_status == status:
            print('.', end='')
        else:
            print(status, end='')
        sys.stdout.flush()
        time.sleep(10)
    print()