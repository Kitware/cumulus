# Set up cluster-specific variables
PARAVIEW_DIR={{paraviewInstallDir if paraviewInstallDir else "/opt/paraview/install"}}
# This file will be written by the pvw.sh script
RC_PORT=`cat /tmp/{{pvwJobId}}.rc_port`

# Run in MPI mode
MPIPROG="mpiexec"
PV_SERVER="${PARAVIEW_DIR}/bin/pvserver"

# Need to adjust paths for Mac application install
if [[ "${PARAVIEW_DIR}" == *paraview.app ]]
then
   PV_SERVER="${PARAVIEW_DIR}/Contents/bin/pvserver"
   MPIPROG="${PARAVIEW_DIR}/Contents/MacOS/mpiexec"
fi

# Wait for pvpython
while ! nc -z ${SGE_O_HOST} ${RC_PORT}; do sleep 1; done
# Now run pvserver and tell it to reverse connect
${MPIPROG} {{ '-n %d' % ((numberOfSlots|int)-1) if numberOfSlots }} ${PV_SERVER} --client-host=${SGE_O_HOST} -rc --server-port=${RC_PORT}
# Clean up port file
rm /tmp/{{pvwJobId}}.rc_port
