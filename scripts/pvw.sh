#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
#$ -q all.q@master

# Set up cluster-specific variables
PARAVIEW_DIR={{paraview_dir if paraview_dir else "/opt/paraview/install"}}
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
LIB_VERSION_DIR=`ls ${PARAVIEW_DIR}/lib`
APPS_DIR="lib/${LIB_VERSION_DIR}/site-packages/paraview/web"
VISUALIZER="${PARAVIEW_DIR}/${APPS_DIR}/pv_web_visualizer.py"

RC_PORT="54321"
REVERSE="--reverse-connect-port ${RC_PORT}"

PROXIES="config/defaultProxies.json"
DATA="{{ dataDir if dataDir else '$HOME/%s/data/' % job._id }}"

# Get the private ip of this host
IPADDRESS=`curl -s http://169.254.169.254/latest/meta-data/local-ipv4`

if [ -z "$IPADDRESS" ];
IPADDRESS=${HOSTNAME}
fi

# Create proxy entry
BODY='{"host": "'$IPADDRESS'", "clusterId": "{{ cluster._id }}", "port": 8080, "jobId": "{{ job._id }}"}'
curl --silent --show-error -o /dev/null -X POST -d "$BODY"  --header "Content-Type: application/json" {{ baseUrl }}/proxy

export LD_LIBRARY_PATH=${PARAVIEW_DIR}/lib/${LIB_VERSION_DIR}
export DISPLAY=:0

# First run pvpython with the reverse connect port
${PV_PYTHON} ${VISUALIZER} --timeout 999999 --host $IPADDRESS --port 8080 --proxies ${PROXIES} ${REVERSE} --data-dir ${DATA} --group-regex ""

# Remove proxy entry
curl --silent --show-error -o /dev/null -X DELETE {{ baseUrl }}/proxy/{{ cluster._id }}/{{ job._id }}
