#!/bin/bash

###
### Run this script with:
###
###   run.sh
###
#$ -S /bin/bash
#$ -pe orte {{ number_of_slots }}

# Set up cluster-specific variables
PARAVIEW_DIR="/opt/paraview/install"
PV_PYTHON="${PARAVIEW_DIR}/bin/pvpython"
APPS_DIR="lib/paraview-4.2/site-packages/paraview/web"
RC_PORT="54321"

# Run in MPI mode
MPIPROG="mpiexec"
PV_SERVER="${PARAVIEW_DIR}/bin/pvserver"

# Now run pvserver and tell it to reverse connect
${MPIPROG} -n ${NB_PROCESSES} ${PV_SERVER} -rc --server-port=${RC_PORT}