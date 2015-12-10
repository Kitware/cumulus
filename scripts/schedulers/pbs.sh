#
#PBS -N {{job.name}}-{{job._id}}
{% if numberOfSlots -%}
#PBS -l procs={{numberOfSlots}}
{% endif -%}
cd $PBS_O_WORKDIR

