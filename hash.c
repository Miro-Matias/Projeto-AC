//arquivo rash.c
#include "jcstore_client.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "pthread.h"
#include "unistd.h"

Node *hashTable[HT_SIZE];
RequestBuffer buffer[NUM_THREADS];

int hash(char* key)
{
	int i = 0;
	if(key == NULL) return -1;
	while(*key != '\0')
	{
		i += (int) *key;
		key++;
	}

	i = i % HT_SIZE;

	return i;
}


int jcstore_init(int num_server_threads,int buf_size,int num_shards)
{
	int i;
	//Aqui estamos a inicializar a tabela de dispersao e o buffer compartilhado
	for(i = 0;i<NUM_THREADS;i++)
	{
		hashTable[i] == NULL;
	}
	for(i=0;i<NUM_THREADS;i++)
	{
		pthread_mutex_init(&buffer[i].mutex,NULL);
		pthread_cond_init(&buffer[i].cond,NULL);
		buffer[i].responseReady = 0;
	}
	return 0;
}

char* jcstore_get(int clientid, int shardid, char* key)
{
	int index =hash(key);
	if(index == -1)
	return NULL;
	Node *current=hashTable[index];
	Node *prev = NULL;
	while(current != NULL)
	{
		if(strcmp(current->kv.key,key)==0)
		{
			return current->kv.value;
		}
		current = current->next;	
	}
	return NULL;
}

KV_t* jcstore_getAllKeys(int clientId,int shardId,int *sizeKVarray)
{
	 KV_t *result= (KV_t*)malloc(HT_SIZE * sizeof(KV_t));
	 int count =0,i=0;
	 for(i=0;i < HT_SIZE;i++)
	 {
	 	Node *current = hashTable[i];
	 	while(current != NULL)
	 	{
	 	  result[count++] =current->kv;
	 	  current = current->next;
	 	}
	 }
	 *sizeKVarray = count;
	 return result;
}


char* jcstore_remove(int clientid, int shardid, char* key)
{
	int index = hash(key);
	if(index == -1)return NULL;
	Node *current = hashTable[index];
	Node *prev = NULL;
	while(current !=NULL)
	{
	   if(strcmp(current->kv.key,key)== 0)
	   {
		   char *value = current->kv.value;
		   if(prev == NULL)
			   hashTable[index]=current->next;
		   else
		           prev->next = current->next;
			   free(current);
			   return value;
		    prev = current;
		    current = current->next;;
	    }
	}
		   return NULL;
}

// Inserindo Dados😎️😎️
char* jcstore_put(int clientid, int shardid, char* key, char* value)
{
	int index = hash(key);
	if(index == -1)
		return NULL;
	Node *current = hashTable[index];
	Node *prev = NULL;
	while(current != NULL)
	{
	   if(strcmp(current->kv.key,key)== 0)
	   {
		     char *oldvalue = current->kv.value;
		     strncpy(current->kv.value,value,KV_SIZE);
	     	     return oldvalue;
	   }
	    prev = current;
	    current = current->next;
	    
	}
	Node *newNode = (Node *)malloc(sizeof(Node));
	strncpy(newNode->kv.key,key,KV_SIZE);
	strncpy(newNode->kv.value,value,KV_SIZE);
	newNode->next = NULL;
	
	if(prev == NULL)
 		hashTable[index] = newNode;
	else
		prev->next =current->next;
		
	 return NULL;
}







