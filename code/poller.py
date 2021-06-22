import logging
import sys
import time

def status_poller(intro, done_status, func, logger):

    logger.setLevel(logging.WARNING)
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
            break            
        sys.stdout.flush()
        time.sleep(30)
    print()
    logger.setLevel(logging.INFO)