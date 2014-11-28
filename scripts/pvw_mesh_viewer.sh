#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
#$ -q all.q@master

# Set up cluster-specific variables
PARAVIEW_DIR="/opt/paraview/install"
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
APPS_DIR="lib/paraview-4.2/site-packages/paraview/web"
VISUALIZER="pv_mesh_viewer.py"
VISUALIZER_URL="https://raw.githubusercontent.com/cjh1/cmb-web/master/scripts/hydra-ne/pv_mesh_viewer.py"
#PROXIES="/opt/paraview/defaultProxies.json"
RC_PORT="54321"
REVERSE="--reverse-connect-port ${RC_PORT}"
MESH="surface.vtk"

# Get the private ip of this host
IPADDRESS=`curl -s http://169.254.169.254/latest/meta-data/local-ipv4`

# Create proxy entry
BODY='{"host": "'$IPADDRESS'", "clusterId": "{{ cluster._id }}", "port": 8080, "jobId": "{{ job._id }}"}'
curl --silent --show-error -o /dev/null -X POST -d "$BODY"  --header "Content-Type: application/json" {{ base_url }}/proxy

export LD_LIBRARY_PATH=$PARAVIEW_DIR/lib/paraview-4.2

# Get the vis script
curl "$VISUALIZER_URL" -o "$VISUALIZER"

# First run pvpython with the reverse connect port
${PV_PYTHON} ${VISUALIZER} --host $IPADDRESS --port 8080 --mesh ${MESH} ${REVERSE}

# Remove proxy entry
curl --silent --show-error -o /dev/null -X DELETE {{ base_url }}/proxy/{{ cluster._id }}/{{ job._id }}
