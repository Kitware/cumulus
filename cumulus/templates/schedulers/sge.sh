
#
#$ -S /bin/bash
#$ -N {{job.name}}-{{job._id}}
{% if parallelEnvironment -%}
#$ -pe {{ parallelEnvironment }} {{ numberOfSlots }}
{% endif -%}
{% if maxWallTime -%}
#$ -l h_rt={{maxWallTime.hours}}:{{maxWallTime.minutes}}:{{maxWallTime.seconds}}
{% endif -%}
{% if gpus -%}
#$ -l gpus={{gpus}}
{% endif -%}
{% if queue -%}
#$ -q {{queue}}
{% endif -%}
{% if account -%}
#$ -A {{account}}
{% endif -%}

export HYDRA_PROXY_RETRY_COUNT=100
# export HYDRA_DEBUG=1

cd $SGE_O_WORKDIR
