#!/bin/bash

HYDRA_DIR="/opt/hydra"
HYDRA="${HYDRA_DIR}/bin/hydra"
MPIPROG="mpiexec"

${MPIPROG} -n  {{ number_of_slots-1 }} ${HYDRA} -i input/dhbox.exo -c input/dhbbox.cntl

rm -rf input