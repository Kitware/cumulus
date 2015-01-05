#!/bin/bash

HYDRA_DIR="/opt/hydra"
HYDRA="${HYDRA_DIR}/bin/hydra"
MPIPROG="mpiexec"

${MPIPROG} -n  {{ number_of_slots }} ${HYDRA} -i input/dhbox.exo -c input/dhbox.cntl

rm -rf input