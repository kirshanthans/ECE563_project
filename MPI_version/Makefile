CC_OMP := mpicc
CC_MPI := mpicxx
LIB := 
CFLAGS := -fopenmp

all :
	$(CC_OMP) $(CFLAGS) -c omp_map_reduce.c
	$(CC_MPI) $(CFLAGS) -c mpi_wc.cpp
	$(CC_MPI) $(CFLAGS) -c mpi_seq.cpp
	$(CC_MPI) $(CFLAGS) omp_map_reduce.o mpi_wc.o -o mpi_wc
	$(CC_MPI) $(CFLAGS) omp_map_reduce.o mpi_seq.o -o mpi_seq

clean :
	rm -f mpi_wc omp_map_reduce.o mpi_wc.o Output_* Result_* mpi_seq mpi_seq.o Serial.txt
	

