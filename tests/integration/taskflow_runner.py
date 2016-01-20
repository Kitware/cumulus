import time
import json

import argparse
from girder_client import GirderClient, HttpError

def main(config):
    client = GirderClient(apiUrl=config.girder_api_url)
    client.authenticate(config.girder_user,
                        config.girder_password)

    try:
        url = 'taskflows'
        body = {
            'taskFlowClass': 'cumulus.mytaskflow.MyTaskFlow'
        }
        r = client.post(url, data=json.dumps(body))
        taskflow_id = r['_id']

        url = 'taskflows/%s/start' % (taskflow_id)
        r = client.put(url)

        status = None
        status_url = url = 'taskflows/%s/status' % (taskflow_id)
        tasks_url = url = 'taskflows/%s/tasks' % (taskflow_id)
        while status != 'complete':
            r = client.get(status_url)
            status = r['status']
            print('Taskflow status:s %s' % status)
            r = client.get(tasks_url)
            print('Tasks in flow: %d' % len(r))
            time.sleep(1)
    except HttpError as ex:
        print ex.responseText
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run test')
    parser.add_argument('-a', '--girder_api_url', help='', required=True)
    parser.add_argument('-u', '--girder_user', help='', required=True)
    parser.add_argument('-p', '--girder_password', help='', required=True)
    args = parser.parse_args()

    main(args)




