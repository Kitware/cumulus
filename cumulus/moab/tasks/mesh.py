from cumulus.starcluster.tasks.celery import command
from cumulus.starcluster.tasks.common import _check_status
import cumulus
import tempfile
import os
import sys
import requests
import json
import paraview.simple as paraview
import traceback
from StringIO import StringIO
from requests_toolbelt import MultipartEncoder

plugin_path = cumulus.config.moabReader.pluginPath
girder_url  = cumulus.config.girder.baseUrl

max_chunk_size = 1024 * 1024 * 64

def _check_status(request):
    if request.status_code != 200:
        if request.headers['Content-Type'] == 'application/json':
            print >> sys.stderr, request.json()
        request.raise_for_status()

def _upload_file(token, base_url, parent_id, name, data, size):
    headers = {'Girder-Token': token}

    params = {
        'parentType': 'item',
        'parentId': parent_id,
        'name': name,
        'size': size
    }

    r = requests.post( '%s/file' % base_url, params=params, headers=headers)
    _check_status(r)
    obj = r.json()

    if '_id' in obj:
        upload_id = obj['_id']
    else:
        raise Exception('Unexpected response: ' + json.dumps(obj))

    uploaded = 0

    while (uploaded != size):

        chunk_size = size - uploaded
        if chunk_size > max_chunk_size:
            chunk_size = max_chunk_size

        part = data.read(chunk_size)

        m = MultipartEncoder(
          fields=[('uploadId',  upload_id),
                  ('offset', str(uploaded)),
                  ('chunk', (name, part, 'application/octet-stream'))]

        )

        headers['Content-Type'] = m.content_type

        r = requests.post('%s/file/chunk' % base_url, params=params,
                                 data=m, headers=headers)
        _check_status(r)

        uploaded += chunk_size

@command.task
def extract(girder_token, mesh_file_id, output):
    try:
        headers = {'Girder-Token': girder_token}
        # First download the mesh
        with tempfile.NamedTemporaryFile() as mesh_file, \
             tempfile.NamedTemporaryFile(suffix='.vtk')  as surface_mesh_file:
            mesh_file_name = os.path.basename(mesh_file.name)
            url = '%s/file/%s/download' % (cumulus.config.girder.baseUrl, mesh_file_id)
            r = requests.get(url, headers=headers)
            _check_status(r)
            mesh_file.write(r.content)
            mesh_file.flush()

            # Now extract the mesh
            paraview.LoadPlugin(plugin_path, ns=globals())
            reader = CmbMoabSolidReader(FileName=mesh_file.name)
            reader.UpdatePipeline()
            paraview.SaveData(surface_mesh_file.name, proxy=reader)

            output_name = output['name']

            # Now upload the surface mesh
            with open(surface_mesh_file.name) as fp:
                size = os.path.getsize(surface_mesh_file.name)
                _upload_file(girder_token, girder_url,  output['itemId'], output['name'], fp, size)
    except:
        exp_str = traceback.format_exc()
        print >> sys.stderr, exp_str
        output_name = '%s.error' % output['name']
        _upload_file(girder_token, girder_url, output['itemId'], output_name, StringIO(exp_str), len(exp_str))




