#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
### TODO template this out for trad
###$ -q all.q@master

# Set up cluster-specific variables
PARAVIEW_DIR={{paraviewInstallDir if paraviewInstallDir else "/opt/paraview/install"}}
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
LIB_VERSION_DIR=`ls ${PARAVIEW_DIR}/lib | grep paraview`
APPS_DIR="lib/${LIB_VERSION_DIR}/site-packages/paraview/web"
VISUALIZER="${PARAVIEW_DIR}/${APPS_DIR}/pv_web_visualizer.py"
GET_PORT_PYTHON_CMD='import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
RC_PORT=`python -c "${GET_PORT_PYTHON_CMD}"`
echo ${RC_PORT} > /tmp/{{job._id}}.rc_port

REVERSE="--reverse-connect-port ${RC_PORT}"

PROXIES="config/defaultProxies.json"
DATA="{{ dataDir if dataDir else '$HOME/%s/data/' % job._id }}"

# Get the private ip of this host
IPADDRESS=`curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/local-ipv4`

if [ -z "$IPADDRESS" ]; then
IPADDRESS=${HOSTNAME}
fi

WEBSOCKET_PORT=`python -c "${GET_PORT_PYTHON_CMD}"`

# Create proxy entry
BODY='{"host": "'$IPADDRESS'", "clusterId": "{{ cluster._id }}", "port": '${WEBSOCKET_PORT}', "jobId": "{{ simulationJobId }}"}'
curl --silent --show-error -o /dev/null -X POST -d "$BODY"  --header "Content-Type: application/json" {{ baseUrl }}/proxy

export LD_LIBRARY_PATH=${PARAVIEW_DIR}/lib/${LIB_VERSION_DIR}
export DISPLAY=:0

# First run pvpython with the reverse connect port
${PV_PYTHON} ${VISUALIZER} --timeout 999999 --host $IPADDRESS --port ${WEBSOCKET_PORT} --proxies ${PROXIES} ${REVERSE} --data-dir ${DATA} --group-regex ""

# Remove proxy entry
curl --silent --show-error -o /dev/null -X DELETE {{ baseUrl }}/proxy/{{ cluster._id }}/{{ simulationJobId }}
