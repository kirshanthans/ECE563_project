#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
#define fnum 160
#define TableSize 5000

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

void reducer(char * fileName,int start,int end,struct hashMap ** maps)
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
				fprintf(f,"%s:%d\n",v,c);
			}	
		}

	}

	fclose(f);



}



int main(int argc, char ** argv)
{


	char * inputFiles[fnum] = {
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/600.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/986.txt.utf-8.txt", 
	"/home/eghabash/Downloads/FinalProject/RawText/1399.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/34114.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/27916.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/36034.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39288.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39294.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39295.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39296.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt",
	"/home/eghabash/Downloads/FinalProject/RawText/39297.txt.utf-8.txt"
};
		
	int i;
	int numOfThreads=atoi(argv[1]);
	struct queue ** queues;
	struct hashMap ** maps;
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
	
	exectime1 = -omp_get_wtime();
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
	
	int blockSize=TableSize/atoi(argv[1]);
	#pragma omp parallel num_threads(numOfThreads)
	{
		char Fname[15];
		sprintf(Fname, "OutputR%d.txt",omp_get_thread_num());
		if(omp_get_thread_num()==atoi(argv[1])-1){
			reducer(Fname,blockSize*omp_get_thread_num(),TableSize,maps);
		}
		else
		{
			reducer(Fname,blockSize*omp_get_thread_num(),blockSize*omp_get_thread_num()+blockSize,maps);
		}
	}
	exectime1 += omp_get_wtime();
	printf("No. Of Threads:%d, Parallel Time:%f\n",atoi(argv[1]),exectime1);
	
	
	
	
	
	for(i=0;i<fnum;i++)
	{
		free(queues[i]);
	}
	free(queues);


	return EXIT_SUCCESS;
}
