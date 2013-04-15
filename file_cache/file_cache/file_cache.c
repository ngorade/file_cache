#include "file_cache.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mman.h"
#include "error.h"


#define FILESIZE 10240
/*
 *	Idea is to keep all pinned files towards rear end of doubly linked cache.
 *	if file in unpinned bby all users, lets keep it in (front end of doubly linked list ) cache untill cache becomes full. now we have 2 cases - 
 *		1.	subsequent pin request is for unpinned (but already in cache) file  --> Use already cached file & pin it. 
 *		2. 	Cache becomes full & Pin request is for file not in cache --> Replace entry for unpinned(in cache file) from front end of linked list.  
 */

typedef struct Node {
	int dirty ;
	int pinCount;
	struct Node *next, *prev;
	char * fileName;
	char* addr;
	pthread_mutex_t mutex;
	int fd;
}Node;
/*
 * typedef struct hash {
 * int limitC; // Not required
 * struct Node *next, *prev;
 * }Hash;
 * */

struct file_cache {
	int totalPinCount ; // number of pinned files
	int availableCache;  // max number of files that can be pinned
	int totalFilesIn;
	Node *head, *rear;
//	Node* *FHash;
};
typedef struct file_cache file_cache;

file_cache *file_cache_construct(int max_cache_entries) {
	int i;
	file_cache *cache = (file_cache *)malloc (sizeof(file_cache));
	cache->availableCache = max_cache_entries;
	cache->totalFilesIn = 0;
	cache->totalPinCount = 0;
	cache->head = cache->rear=NULL;
	return cache;
}

Node *searchPinned(file_cache *cache, const char *file) {
	Node* walker = cache->rear;
	while (walker !=NULL && walker->pinCount)  { // all pinned files will be towards rear end
		if(!strcmp(walker->fileName,file)) {
			return walker;
		}
		walker = walker->prev;
	}
	return NULL;
}

Node *searchInNodes(file_cache *cache, const char *file) {
	Node* walker = cache->rear;
	while (walker !=NULL )  { 
		if(!strcmp(walker->fileName,file)) {
			return walker;
		}
		walker = walker->prev;
	}
	return NULL;
}

int isFull(file_cache *cache) {

	if (cache->totalFilesIn == cache->availableCache) return 1;
	else return 0;
}

int allPinned(file_cache *cache) {
	return cache->head->pinCount;	// if all the cached files are pinned then ->head node's pinCount > 0
}

void file_cache_pin_files(file_cache *cache, const char **files, int num_files) {
	// Assumed a absolute file path is mentioned
	int item;
       	//int fd;
	int len;
	Node *temp;
	char *buf;
	for (item=0;item < num_files; item++) {

		printf("\n PIN file :- \"%s\" \n",files[item]);
		temp = searchInNodes(cache, files[item]);  // if File already in cache (pinned/non-dirty unpinned) 
	
		if (temp == NULL) {
			// File is not in cache
			printf("	File %s is not in cache \n",files[item]);
			if (isFull(cache) && allPinned(cache)) { 	// is there space to pin ?
				printf("	** Cache is fullll , cannnot add further\n");
				return; // TO BE handled at application 
			} else if (isFull(cache)) {
				// if cache is full but few entries are unpinned, replace those entries with new files.
				printf("	Cache is full with few unpinned file. lets replace one of the unpinned file\n");
//				munmap(temp->addr, FILESIZE);
				temp=cache->head;
				close(temp->fd);
				pthread_mutex_lock (&temp->mutex);

				if (cache->rear == cache->head) {

					// only one node , i.e. max size of cache is 1, do nothing
				} else {
					cache->head = temp->next;
					temp->next->prev=NULL;
				}
					pthread_mutex_unlock (&temp->mutex);
			} else {	
				
				// there is space to pin,  create new Node, & add it towards rear end of dobly linked list.
				// approach is - add new node instead of immediately replacing unpinned files. remove unpinned files
				// only when cache is full.
				printf("	Making new node .. \n");
				temp = (Node *) malloc (sizeof(Node));
				temp->prev = cache->rear;
				cache->totalFilesIn++;
				len = strlen(files[item]);
				temp->fileName = malloc (sizeof(char) * len );
				//temp->mutex = PTHREAD_MUTEX_INITIALIZER;
				pthread_mutex_init(&temp->mutex,NULL);
			}
			pthread_mutex_lock (&temp->mutex);
			if (cache->rear) {	
				cache->rear->next= temp;
			}
			cache->rear=temp;
		        temp->next = NULL;
			temp->dirty = 0;
			strcpy(temp->fileName,files[item]);
			temp->pinCount = 1;
			cache->totalPinCount++;
			temp->fd = open(files[item], O_RDWR , S_IRWXU);
		        if (temp->fd == -1) {
				printf("	File %s is not available, creating the new (of size 10kb & 0 filled ) ..\n ", files[item]);
				temp->fd = open(files[item], O_RDWR|O_CREAT ,S_IRWXU);
				buf = (char *) malloc (sizeof(char)*FILESIZE);
				memset(buf,'0',10240);
				write(temp->fd,buf,10240);
				free(buf);
				fsync(temp->fd);
			}

			temp->addr = mmap(NULL, FILESIZE, PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, temp->fd, 0);
			if (temp->addr == MAP_FAILED) { perror("mmap"); }
			if (cache->head == NULL) cache->head = temp;
			printf("	Pinned new file \"%s\"\n", temp->fileName);
			pthread_mutex_unlock (&temp->mutex);
		} else {
			pthread_mutex_lock (&temp->mutex);
	     		if (temp->pinCount) {
				printf("	File is already pinned in the cache,  increasing pinCount \n");
			} else {
				printf("	File is already in the cache (though unpinned),  increasing pinCount \n");
			}
	     		(temp->pinCount)++;
	
	     		// Take it to rear end the queue,
	     		if (temp != cache->rear) {
	     			// if fileNode is *not* already at the rear
				if (temp->prev == NULL) {
					// if temp is at head
					cache->head = temp->next;
					temp->next->prev = NULL;
				} else {
					// if temp is neither at head nor at rear
					temp->prev->next = temp->next;
					temp->next->prev = temp->prev;
				}
				temp->prev = cache->rear;
				cache->rear = temp;
				temp->next  = NULL;
				temp->prev->next = temp;
			}
			pthread_mutex_unlock (&temp->mutex);
			printf("	Increased pincount of file \"%s\" to %d\n", temp->fileName, temp->pinCount);
	     	}
	}
	// succeffully pinned all files,
	//	return 0;
}


void file_cache_unpin_files(file_cache *cache,
	const char **files,
	int num_files) {

	int item;
	Node *temp;
	int pageSize = sysconf(_SC_PAGE_SIZE);
	
	for (item=0;item < num_files; item++) {
		printf("\n UNPIN file :- \"%s\" \n",files[item]);
		temp = searchPinned(cache, files[item]);    // hypothetical hashFunction returns Node * is file is in cache.
		if (temp != NULL) {
			pthread_mutex_lock (&temp->mutex);
			temp->pinCount--;
			printf("	decremented pincount of file \"%s\" to %d\n", temp->fileName, temp->pinCount);
			if (!temp->pinCount) {
				//pinCount has become 0, move it's entry to fron of doubly linked list.
				if (temp->prev == NULL) {
					// if temp is at head
					// do Nothing
				} else if (temp->next == NULL) {
				    	// if temp is at rear-end
			        	cache->rear = temp->prev;
					temp->prev->next = temp->next;
					temp->next = cache->head;
					cache->head->prev = temp;
					cache->head = temp;
					temp->prev = NULL;
				} else {
					//if TEMP IS IN BETWEEN
					temp->prev->next = temp->next;
					temp->next->prev = temp->prev;
					temp->next = cache->head;
					cache->head->prev = temp;
					cache->head = temp;
					temp->prev = NULL;
				}
				if (temp->dirty) {
					// write back the file
					msync(temp->addr, FILESIZE, MS_SYNC);
					munmap(temp->addr, FILESIZE);
				}
				cache->totalPinCount--;
			printf("	Unpinned file \"%s\" \n", temp->fileName);
			}
			pthread_mutex_unlock (&temp->mutex);
		} else {
			// file was not pinned,  leave it to app to handle.
		}
	}
}

const char *file_cache_file_data(file_cache *cache, const char *file) {
	Node* node = searchPinned(cache,file) ;
	char* locnOfFile = NULL;
	int pageSize = sysconf(_SC_PAGE_SIZE);
	if (node) {
		mprotect(node->addr, FILESIZE,PROT_READ);
		node->dirty = 0; // no need to do
	}
	return node->addr;
}

char *file_cache_mutable_file_data(file_cache *cache, const char *file) {
	Node* node = searchPinned(cache,file) ;
	char* locnOfFile = NULL;
	int pageSize = sysconf(_SC_PAGE_SIZE);
	if (node) {
		mprotect(node->addr, FILESIZE,PROT_WRITE);
		node->dirty = 1;
	}
	return node->addr;
}

void print (file_cache *cache) {
	Node *node = cache->head;

	printf ("\n <============ CACHE IS ==============>");
	while (node !=NULL) {
		printf ("\n File %s, pin %d", node->fileName, node->pinCount);
		node = node->next;
	}
	printf ("\n **************************************\n"); 
}

int main (int argc, char argv[]) {
	file_cache *cache;
	char **filenames, **filenames2, **filenames3, **filenames4, *pointer;
	//	Sample to code to test implemented apis.
	cache = file_cache_construct(3);
	//--------------> files t1,t2
	filenames = malloc (sizeof(char *) * 2);
	filenames[0] =  malloc (sizeof(char ) * 3);
	filenames[1] =  malloc (sizeof(char ) * 3);
	strcpy(filenames[0],"t1");
	strcpy(filenames[1],"t2");
	//<------------- files t1

	//--------------> files t1
	filenames2 = malloc (sizeof(char *) * 1);
	filenames2[0] =  malloc (sizeof(char ) * 3);
	strcpy(filenames2[0],"t1");
	//<------------- files t1

	//--------------> files t1
	filenames3 = malloc (sizeof(char *) * 1);
	filenames3[0] =  malloc (sizeof(char ) * 3);
	strcpy(filenames3[0],"t3");
	//<------------- files t1

	//--------------> files t1
	filenames4 = malloc (sizeof(char *) * 1);
	filenames4[0] =  malloc (sizeof(char ) * 3);
	strcpy(filenames4[0],"t4");
	//<------------- files t1
	file_cache_pin_files(cache,(const char **)filenames,2);
	print(cache);
	file_cache_pin_files(cache,(const char **)filenames,2);
	print(cache);
	file_cache_pin_files(cache,(const char **)filenames3,1);
	file_cache_pin_files(cache,(const char **)filenames2,1);
	print(cache);
	file_cache_unpin_files(cache,(const char **)filenames2,1);
	print(cache);

	file_cache_unpin_files(cache,(const char **)filenames2,1);
	print(cache);
	file_cache_unpin_files(cache,(const char **)filenames2,1);
	print(cache);
	file_cache_pin_files(cache,(const char **)filenames4,1);
	print(cache);
}
