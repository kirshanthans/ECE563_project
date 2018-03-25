CC_OMP := gcc
CC_MPI := mpicxx
LIB := 

all : mpi_wc

mpi_wc : mpi_wc.cpp
	$(CC_MPI) $(LIB) -o $@ $< 


clean :
	rm -f mpi_wc

