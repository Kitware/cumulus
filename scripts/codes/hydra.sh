HYDRA_DIR="/opt/hydra"
HYDRA="{{ hydraExecutablePath if hydraExecutablePath else '${HYDRA_DIR}/bin/hydra' }}"
MPIPROG="mpiexec"
mkdir output
${MPIPROG} {{ '-n %d' % numberOfSlots if numberOfSlots }} ${HYDRA} -i input/{{mesh.name}} -c input/hydra.cntl -p output/results.exo -o output/log.txt -g output/stat.txt
rm -rf input/{{mesh.name}}
