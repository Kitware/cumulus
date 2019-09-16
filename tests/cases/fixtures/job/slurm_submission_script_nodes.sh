#!/bin/sh
#                             _
#                            | |
#   ___ _   _ _ __ ___  _   _| |_   _ ___
#  / __| | | | '_ ` _ \| | | | | | | / __|
# | (__| |_| | | | | | | |_| | | |_| \__ \
#  \___|\__,_|_| |_| |_|\__,_|_|\__,_|___/
#
#
#SBATCH --job-name=dummy-123432423
#SBATCH --output=dummy-123432423.o%j
#SBATCH --error=dummy-123432423.e%j
#SBATCH --chdir=
#SBATCH --nodes=12312312

ls
sleep 20
mpirun -n 1000000 parallel
