#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
//#define fnum 160
#define TableSize 5000

// for sending filenames
#define FNUM 10
#define FLEN 256
#define NTHREADS 8

// data structures
struct hashNode
{
	char * value;
	int count;
	struct hashNode * next;
		
}hashNodeT;


struct hashMap
{
	struct hashNode ** table;
		
}hashMapT;



struct node
{
	struct node * next;
	char * line;
}qnode;

struct queue
{
	struct node * head;
	struct node * tail;
	bool noMoreWork;
}tqueue;

// functions
int hash(char* str);
int get(struct hashMap ** map,char * value);
void put(struct hashMap ** map,char * value);
void fillQueue(char * file,struct queue ** queues,int lockIndex);
void mapInput(struct queue ** queues,struct hashMap ** map);
void reducer(char * fileName,int start,int end,struct hashMap ** maps, int fnum);
//int run_omp(int fnum, char** files, int nThreads);
