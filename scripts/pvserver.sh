#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
{% if parallel_environment -%}
#$ -pe {{ parallel_environment }} {{ number_of_slots-1 }}
{% endif -%}

# Set up cluster-specific variables
PARAVIEW_DIR="/opt/paraview/install"
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
APPS_DIR="lib/paraview-4.2/site-packages/paraview/web"
RC_PORT="54321"

# Run in MPI mode
MPIPROG="mpiexec"
PV_SERVER="${PARAVIEW_DIR}/bin/pvserver"
# Wait for pvpython
while ! nc -z master ${RC_PORT}; do sleep 1; done
# Now run pvserver and tell it to reverse connect
${MPIPROG} {{ '-n %d' % number_of_slots-1 if number_of_slots }} ${PV_SERVER} --client-host=master -rc --server-port=${RC_PORT}
