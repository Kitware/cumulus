#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
{% if cluster.type == 'ec2' -%}
#$ -q all.q@master
{% endif -%}

# Set up cluster-specific variables
PARAVIEW_DIR={{paraviewInstallDir if paraviewInstallDir else "/opt/paraview/install"}}
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
LIB_VERSION_DIR=`ls ${PARAVIEW_DIR}/lib | grep paraview`
APPS_DIR="lib/${LIB_VERSION_DIR}/site-packages/paraview/web"
VISUALIZER="/opt/hpccloud/hpccloud/scripts/hydra-th/pv_mesh_viewer.py"

# Get the private ip of this host
IPADDRESS=`curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/local-ipv4`

if [ -z "$IPADDRESS" ]; then
IPADDRESS=${HOSTNAME}
fi

GET_PORT_PYTHON_CMD='import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
WEBSOCKET_PORT=`python -c "${GET_PORT_PYTHON_CMD}"`

# Create proxy entry
BODY='{"host": "'$IPADDRESS'", "clusterId": "{{ cluster._id }}", "port": '${WEBSOCKET_PORT}', "jobId": "{{ jobId }}"}'
curl --silent --show-error -o /dev/null -X POST -d "$BODY"  --header "Content-Type: application/json" {{ baseUrl }}/proxy

export LD_LIBRARY_PATH=${PARAVIEW_DIR}/lib/${LIB_VERSION_DIR}
export DISPLAY=:0

# First run pvpython with the reverse connect port
${PV_PYTHON} ${VISUALIZER} --timeout 300 --host $IPADDRESS --port ${WEBSOCKET_PORT} -f --token {{girderToken}} --url {{ baseUrl }} --file {{ fileId }} --item {{ itemId }}

# Remove proxy entry
curl --silent --show-error -o /dev/null -X DELETE {{ baseUrl }}/proxy/{{ cluster._id }}/{{ jobId }}
