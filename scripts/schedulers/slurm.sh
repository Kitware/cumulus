#
#SBATCH --job-name={{job.name}}-{{job._id}}
{% if numberOfSlots -%}
#SBATCH --ntasks={{numberOfSlots}}
{% elif numberOfNodes -%}
#SBATCH --nodes={{numberOfNodes}}
{% endif -%}
{% if numberOfNodes and numberOfCoresPerNode -%}
{{'#SBATCH --cpus-per-task=%s' % numberOfCoresPerNode if numberOfCoresPerNode }}
{% endif -%}
{% if numberOfNodes and numberOfGpusPerNode -%}
#SBATCH --gres=gpu:{{numberOfGpusPerNode}}
{% endif -%}
{% if maxWallTime -%}
#SBATCH --time=={{maxWallTime.hours}}-{{maxWallTime.minutes}}:{{maxWallTime.seconds}}
{% endif -%}


