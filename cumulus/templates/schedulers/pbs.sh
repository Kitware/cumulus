#
#PBS -N {{job.name}}-{{job._id}}
{% if numberOfSlots -%}
#PBS -l procs={{numberOfSlots}}
{% elif numberOfNodes -%}
#PBS -l nodes={{numberOfNodes}}{{':ppn=%s' % numberOfCoresPerNode if numberOfCoresPerNode}}{{':gpus=%s' % numberOfGpusPerNode if numberOfGpusPerNode}}
{% endif -%}
{% if maxWallTime -%}
#PBS -l walltime={{maxWallTime.hours}}:{{maxWallTime.minutes}}:{{maxWallTime.seconds}}
{% endif -%}
cd $PBS_O_WORKDIR

