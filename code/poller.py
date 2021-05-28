import logging
import sys
import time

def status_poller(intro, done_status, func, emr):

    emr.logger.setLevel(logging.WARNING)
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
    emr.logger.setLevel(logging.INFO)