CC_OMP := gcc
CC_MPI := mpicxx
LIB := 
CFLAGS := -fopenmp

all :
	$(CC_OMP) $(CFLAGS) -c omp_map_reduce.c
	$(CC_MPI) $(CFLAGS) -c mpi_wc.cpp
	$(CC_MPI) $(CFLAGS) *.o -o mpi_wc

clean :
	rm -f mpi_wc omp_map_reduce.o mpi_wc.o OutputR*

