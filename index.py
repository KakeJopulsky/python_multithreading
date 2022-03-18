import json
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

#################################################################################
# Loads a CSV, transforms to JSON, and creates/updates user in Iterable.
# The number of success/failed requests are logged before the program ends.
# Output the fails to a file for better tracking.
#
# Usage: 
#   - ITERABLE_API_KEY: Your Iterable API key
#   - PATH_TO_CSV: Path to your CSV of users
#   - NUM_THREADS: Control how many threads are active at once
#
# Requirements: pip install requests pandas
#
# Author: Jake Kopulsky (03/2022)
#################################################################################

ITERABLE_API_KEY = ""
ITERABLE_API_URL = 'https://api.iterable.com/api/users/update'
PATH_TO_CSV = ''

win = []
loss = []
NUM_THREADS = 10

def fetch(data):
    payload = {
        'email': data['email']
    }
    try:
        payload = json.dumps(payload)
        headers = {
            'Content-Type': 'application/json',
            'Api-Key': ITERABLE_API_KEY
        }
        response = requests.post(ITERABLE_API_URL, headers=headers, data=payload)
        
        win.append(data) if response.status_code == 200 else loss.append(data)
        return response.status_code
    except Exception as err:
        raise SystemExit(err)

def main():
    start_time = time.time()
    threads = []
    df = pd.read_csv(PATH_TO_CSV)
    json_data = json.loads(df.to_json(orient='records'))
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        for payload in json_data:
            threads.append(executor.submit(fetch, payload))

    for task in as_completed(threads):
        # do something
        print (task.result())

    print (f'{len(win)} successful requests')
    print (f'{len(loss)} failed requests')
    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()
