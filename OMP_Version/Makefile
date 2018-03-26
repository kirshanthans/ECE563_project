CFLAGS = -fopenmp 
GCC = gcc $(CFLAGS) 
SRCS = omp_map_reduce.c
OBJS = $(SRCS:%.c=%.o)

all: omp

omp: $(OBJS)
	$(GCC) $(OBJS) -o omp_map_reduce

.c.o:
	$(GCC) -c $*.c

test2:
	./omp_map_reduce 2
test4:
	./omp_map_reduce 4
test8:
	./omp_map_reduce 8
test16:
	./omp_map_reduce 16

clean:
	/bin/rm -f *.txt
	/bin/rm -f *.o
	/bin/rm -f omp_map_reduce
