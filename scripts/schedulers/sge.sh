
#
#$ -S /bin/bash
#$ -N {{job.name}}-{{job._id}}
{% if parallelEnvironment -%}
#$ -pe {{ parallelEnvironment }} {{ numberOfSlots }}
{% endif -%}
{% if maxWallTime -%}
#$ -l h_rt={{maxWallTime}}
{% endif -%}
{% if gpus -%}
#$ -l gpus={{gpus}}
{% endif -%}


cd $SGE_O_WORKDIR

