#!/bin/sh -l
#PBS -q scholar
#PBS -l nodes=1:ppn=16
#PBS -l walltime=01:00:00
module load devel
cd $PBS_O_WORKDIR
./mpiF 16
./mpiF 8
./mpiF 4
./mpiF 2

