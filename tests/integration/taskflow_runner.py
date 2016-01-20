import time
import json

import argparse
from girder_client import GirderClient, HttpError

def wait_for_complete(client, taskflow_id):
    status = None
    status_url = 'taskflows/%s/status' % (taskflow_id)
    tasks_url = 'taskflows/%s/tasks' % (taskflow_id)
    while status != 'complete':
        r = client.get(status_url)
        status = r['status']
        print('Taskflow status: %s' % status)
        r = client.get(tasks_url)
        print('Tasks in flow: %d' % len(r))
        time.sleep(1)

def create_taskflow(client, cls_name):
    url = 'taskflows'
    body = {
        'taskFlowClass': cls_name
    }
    r = client.post(url, data=json.dumps(body))

    return r['_id']

def main(config):
    client = GirderClient(apiUrl=config.girder_api_url)
    client.authenticate(config.girder_user,
                        config.girder_password)

    try:
        # First run the simple flow
        print ('Running simple taskflow ...')
        taskflow_id = create_taskflow(
            client, 'cumulus.mytaskflows.SimpleTaskFlow')

        # Start the task flow
        url = 'taskflows/%s/start' % (taskflow_id)
        client.put(url)

        # Wait for it to complete
        wait_for_complete(client, taskflow_id)

        # Now try something with a chord
        print ('Running taskflow containing a chord ...')
        taskflow_id = create_taskflow(
            client, 'cumulus.mytaskflows.ChordTaskFlow')

        # Start the task flow
        url = 'taskflows/%s/start' % (taskflow_id)
        client.put(url)

        # Wait for it to complete
        wait_for_complete(client, taskflow_id)
    except HttpError as ex:
        print( ex.responseText)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run test')
    parser.add_argument('-a', '--girder_api_url', help='', required=True)
    parser.add_argument('-u', '--girder_user', help='', required=True)
    parser.add_argument('-p', '--girder_password', help='', required=True)
    args = parser.parse_args()

    main(args)




