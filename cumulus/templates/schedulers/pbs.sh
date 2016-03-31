#
#PBS -N {{job.name}}-{{job._id}}
{% if numberOfSlots -%}
#PBS -l procs={{numberOfSlots}}
{% elif numberOfNodes -%}
#PBS -l nodes={{numberOfNodes}}{{':ppn=%s' % numberOfCoresPerNode if numberOfCoresPerNode}}{{':gpus=%s' % numberOfGpusPerNode if numberOfGpusPerNode and numberOfGpusPerNode|int > 0 }}
{% endif -%}
{% if maxWallTime -%}
#PBS -l walltime={{maxWallTime.hours}}:{{maxWallTime.minutes}}:{{maxWallTime.seconds}}
{% endif -%}
{% if queue -%}
#PBS -q {{queue}}
{% endif -%}
{% if account -%}
#PBS -A {{account}}
{% endif -%}
cd $PBS_O_WORKDIR

