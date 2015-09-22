#!/bin/bash

###
#$ -S /bin/bash
{% if parallel_environment -%}
#$ -pe {{ parallel_environment }} {{ number_of_slots }}
{% endif -%}
###

HYDRA_DIR="/opt/hydra"
HYDRA="${HYDRA_DIR}/bin/hydra"
MPIPROG="mpiexec"
mkdir output
${MPIPROG} {{ '-n %d' % number_of_slots if number_of_slots }} ${HYDRA} -i input/{{mesh.name}} -c input/hydra.cntl -p output/results.exo -o output/log.txt -g output/stat.txt
rm -rf -rf input/{{mesh.name}}