#include "omp_map_reduce.h"
#include <mpi.h>

int hash( char *str)
{
	unsigned long hash = 5381;
		int c;
		
		while (c = *str++)
			hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
				
	return hash%TableSize;
}


int get(struct hashMap ** map,char * value)
{
	int hashValue=hash(value);
	struct hashMap * m=*map;
	struct hashNode * node=m->table[hashValue];
	struct hashNode * node2=node;	
	int x;
	if(node==NULL)
		return;
	if(node!=NULL && strcmp(node->value,value)==0)
	{
		
		m->table[hashValue]=m->table[hashValue]->next;
		x=node->count;
		free(node);
		return x;

	}
	node=node->next;
	while(node!=NULL)
	{
		if(strcmp(node->value,value)==0)
		{
			node2->next=node->next;
			int x=node->count;
			free(node);
			return x;
		}
		node=node->next;
		node2=node2->next;
	}
	return 0;
}
void put(struct hashMap ** map,char * value)
{
		
		int i;
		
		int len=strlen(value);
		int c=0;
		char str[len];
		for(i=0;i<len;i++)
		{
			if(isalpha(value[i]) || value[i]=='\'')
			{
				str[c]=tolower(value[i]);
				c++;
			}
		}
		str[c]='\0';
		len=strlen(str);
		char v[len];
		strcpy(v,str);
		int hashValue=hash(v);
		struct hashMap * m=*map;
		struct hashNode * node=m->table[hashValue];
		struct hashNode * temp=malloc(sizeof(hashNodeT));
		temp->value=malloc(sizeof(char)*len+1);
		strcpy(temp->value,v);
		temp->count=1;

		while(node!=NULL)
		{
			if(strcmp(node->value,v)==0)
			{
				node->count++;
					free(temp);
					return;
			}
			node=node->next;
		}
	if(node==NULL)
	{
		temp->next=m->table[hashValue];
			m->table[hashValue]=temp;
			return;
	}
	
		
		
}



void fillQueue(char * file,struct queue ** queues,int lockIndex)
{
	FILE* fp = fopen(file, "r");
	if (fp == NULL) {
		printf("File %s cannot be open\n",file);
		return;
	}
	char * line = NULL;
	size_t len = 0;
	ssize_t read;
	struct queue * temp=*queues;
	while ((read = getline(&line, &len, fp)) != -1) 
	{
		if(temp->tail==NULL)
		{

			temp->tail=malloc(sizeof(qnode));
			temp->tail->next=NULL;
			temp->tail->line=malloc(sizeof(char)*len);
			strcpy(temp->tail->line,line);
			temp->head=temp->tail;

		}
		else
		{
			temp->tail->next=malloc(sizeof(qnode));
			temp->tail->next->line=malloc(sizeof(char)*len);			
			strcpy(temp->tail->next->line,line);
			temp->tail=temp->tail->next;
			temp->tail->next=NULL;		
		}
	}

	temp->noMoreWork=true;
	fclose(fp);
	free(line);



}

void mapInput(struct queue ** queues,struct hashMap ** map)
{
		struct queue * temp=*queues;
		struct node * temp2; // used to free the nodes.

		int c;
		int n;
		





		while(temp->head!=NULL || !temp->noMoreWork)
		{

			if((temp->head==temp->tail && !temp->noMoreWork) || temp->head==NULL)
			{

				continue;
			}



			n = strlen(temp->head->line);
			char str[n];
			strcpy(str,temp->head->line);
			char *token;
    			char *rest = str;
 
   			 while ((token = strtok_r(rest, " ", &rest)))
        		 {
				put(map,token);

 	  		 }

			
			temp2=temp->head;
			temp->head=temp->head->next;
			free(temp2->line);
			free(temp2);

		}


}

void reducer(char * fileName,int start,int end,struct hashMap ** maps, int fnum)
{

	int c;
	int i;	
	int j;
	int k;
	char *v;
	FILE * f=fopen(fileName,"w");
	for(i=0;i<fnum;i++)
	{
		for(j=start;j<end;j++)
		{
			
			while(maps[i]->table[j]!=NULL)
			{
				c=0;
				v=maps[i]->table[j]->value;	
				for(k=0;k<fnum;k++)
				{
					c+=get(&maps[k],v);
				}
				fprintf(f,"%-128s%d\n",v,c);
			}	
		}

	}

	fclose(f);



}



struct hashMap** run_omp(int fnum, char** files, int nThreads, int pid, int numproc)
{


	char ** inputFiles = files;
		
	int i;
	int numOfThreads=nThreads;
	struct queue ** queues;
	struct hashMap ** maps;

    // Commented out the serial implementation for now 
    /*
	queues= malloc(fnum*sizeof(*queues));
	maps= malloc(fnum*sizeof(*maps));
	for(i=0;i<fnum;i++)
	{
		queues[i]=malloc(sizeof(tqueue));
		queues[i]->head=NULL;
		queues[i]->tail=NULL;
		queues[i]->noMoreWork=false;
		maps[i]=malloc(sizeof(hashMapT));
		maps[i]->table=calloc(TableSize,sizeof(struct hashNode*));
	}	

	
	double exectime1 = -omp_get_wtime();
	for(i=0;i<fnum;i++)
		fillQueue(inputFiles[i],&queues[i],i);
	for(i=0;i<fnum;i++)
		mapInput(&queues[i],&maps[i]);
	reducer("Serial.txt",0,TableSize,maps);
	exectime1 += omp_get_wtime();
	printf("Serial Time:%f\n",exectime1);
	*/
	
    queues= malloc(fnum*sizeof(*queues));
	maps= malloc(fnum*sizeof(*maps));
	
	for(i=0;i<fnum;i++)
	{
		queues[i]=malloc(sizeof(tqueue));
		queues[i]->head=NULL;
		queues[i]->tail=NULL;
		queues[i]->noMoreWork=false;

		maps[i]=malloc(sizeof(hashMapT));
		maps[i]->table=calloc(TableSize,sizeof(struct hashNode*));
	}	
	
	omp_set_nested(1);
	#pragma omp parallel sections
	{
		#pragma omp section
		{

			#pragma omp parallel for num_threads(numOfThreads)
			for(i=0;i<fnum;i++)
				fillQueue(inputFiles[i],&queues[i],i);
		}
		#pragma omp section
		{
			#pragma omp parallel for num_threads(numOfThreads)
			for(i=0;i<fnum;i++)
				mapInput(&queues[i],&maps[i]);		
		}
	}
    int blockSize=TableSize/(numproc-1);
    #pragma omp parallel num_threads(numproc-1)
    {
        char Fname[64];
        sprintf(Fname, "Output_pid%d_tid%d.txt", pid, omp_get_thread_num()+1);
        if(omp_get_thread_num()==numproc-1){
            reducer(Fname,blockSize*omp_get_thread_num(),TableSize,maps, fnum);
        }
        else
        {
            reducer(Fname,blockSize*omp_get_thread_num(),blockSize*omp_get_thread_num()+blockSize,maps, fnum);
        }
    }

	
	
	
	
	for(i=0;i<fnum;i++)
	{
		free(queues[i]);
	}
	free(queues);


	return maps;
}

struct hashMap** run_omp_seq(int fnum, char** files)
{


	char ** inputFiles = files;
		
	int i;
	struct queue ** queues;
	struct hashMap ** maps;

    // Commented out the serial implementation for now 
	queues= malloc(fnum*sizeof(*queues));
	maps= malloc(fnum*sizeof(*maps));
	for(i=0;i<fnum;i++)
	{
		queues[i]=malloc(sizeof(tqueue));
		queues[i]->head=NULL;
		queues[i]->tail=NULL;
		queues[i]->noMoreWork=false;
		maps[i]=malloc(sizeof(hashMapT));
		maps[i]->table=calloc(TableSize,sizeof(struct hashNode*));
	}	

	
	/*double exectime1 = -omp_get_wtime();*/
	for(i=0;i<fnum;i++)
		fillQueue(inputFiles[i],&queues[i],i);
	for(i=0;i<fnum;i++)
		mapInput(&queues[i],&maps[i]);
	reducer("Serial.txt",0,TableSize,maps,fnum);
	/*exectime1 += omp_get_wtime();*/
	/*printf("Serial Time:%f\n",exectime1);*/
	
}
