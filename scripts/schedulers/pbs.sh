#
#PBS -N {{job.name}}-{{job._id}}
{% if numberOfSlots -%}
#PBS -l procs={{numberOfSlots}}
{% endif -%}
{% if maxWallTime -%}
#PBS -l walltime={{maxWallTime}}
{% endif -%}
cd $PBS_O_WORKDIR

