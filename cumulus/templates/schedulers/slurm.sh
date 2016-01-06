#
#SBATCH --job-name={{job.name}}-{{job._id}}
#SBATCH --output={{job.name}}-{{job._id}}.o%j
#SBATCH --error={{job.name}}-{{job._id}}.e%j
#SBATCH --workdir={{job.dir}}
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
{% if queue -%}
#SBATCH --partition={{queue}}
{% endif -%}
{% if account -%}
#SBATCH --account={{account}}
{% endif -%}

