#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
{% if parallelEnvironment -%}
#$ -pe {{ parallelEnvironment }} {{ numberOfSlots-1 }}
{% endif -%}

# Set up cluster-specific variables
PARAVIEW_DIR={{paraviewDir if paraviewDir else "/opt/paraview/install"}}
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
LIB_VERSION_DIR=`ls ${PARAVIEW_DIR}/lib | grep paraview`
APPS_DIR="lib/${LIB_VERSION_DIR}/site-packages/paraview/web"
RC_PORT="54321"

# Run in MPI mode
MPIPROG="mpiexec"
PV_SERVER="${PARAVIEW_DIR}/bin/pvserver"
# Wait for pvpython
while ! nc -z ${SGE_O_HOST} ${RC_PORT}; do sleep 1; done
# Now run pvserver and tell it to reverse connect
${MPIPROG} {{ '-n %d' % numberOfSlots-1 if numberOfSlots }} ${PV_SERVER} --client-host=${SGE_O_HOST} -rc --server-port=${RC_PORT}
