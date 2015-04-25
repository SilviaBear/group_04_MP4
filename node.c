#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include "adaptor.h"
#include "transfer_manager.h"
#include "data_structure.h"

//Max number of connections supported, in our case only 1
#define BACKLOG 1

long WORKSIZE = 1024 * 1024 * 32;
long INIT_NUM_OF_JOBS = 2048;
long SIZE_PER_JOB = 1024 * 1024 * 32 / 2048;

//Hostname of remote node for data transmission
char* remote_hostname = NULL;
//Port of remote node for data transmission
char* remote_data_port = NULL;
//Port of remote node for status update
char* remote_control_port = NULL;
//Port of this node to bind for data transmission
char* local_data_port = NULL;
//Port of this node to bind for status update
char* local_control_port = NULL;

int isLocal;

//Sockets for data and control channel
int data_sockfd;
int control_sockfd;
//If remote node, sockets accepted of the local node
int accept_data_sockfd;
int accept_control_sockfd;

/*
Every node will be of initial space of WORKSIZE * 2 to avoid realloc overhead and ensurethe data consistency in memory.
As the local node initiate the workload, the implicit queue structure use the most significant bit of jobs* as queue head.

local node:
jobs                    current job
|------------------------|----------------|
finished                 unfinished
Work direction:---->
remote node:
jobs                    current job
|------------------------|----------------|
unfinished               finished
Work direction:<----
Remote node use the least significant bit of jobs* memory block as queue head

When working, it always works on from thr queue head; when transfering, it always take the jobs from the end of queue; upon receiving, it always put the new task at the end of the queue 
*/
//Jobs vector
double* jobs_head;
//The pointer of the boundary of finished and unfinished jobs
double* queue_head;

extern status_info* remote_status;

//Data structre for remote socket if it is local node
struct sockaddr_storage their_addr;
//Data structure for local socket information if it is remote node
struct addrinfo hints, *servinfo, *p;
socklen_t addr_len;

pthread_mutex_t queue_m = PTHREAD_MUTEX_INITIALIZER;

int setupServerSocket(void* port);
int setupClientSocket();
void* get_in_addr(struct sockaddr *sa);
void* adapter_func();
void transfer_manager_init();
void* transfer_job(int num);
void workload_init();

int main(int argc, char *argv[]) {
  if(argc != 7) {
    //If used as remote node (passive), set local_data_port and local_control_port as "NULL"
    printf("Usage: isLocal, local_data_port, local_control_port, remote_hostname, remote_data_port, remote_control_port\n");
    exit(1);
  }
  isLocal = atoi(argv[1]);
  double* jobs = (double*)malloc(sizeof(double) * WORKSIZE);
  jobs_head = isLocal? jobs : jobs + (INIT_NUM_OF_JOBS - 1) * SIZE_PER_JOB;
  queue_head = jobs_head;
  local_status = (status_info*)malloc(sizeof(status_info));
  remote_status = (status_info*)malloc(sizeof(status_info));
  //If this node is remote, do not need to specify the remote host as it will listen for connections, but need to bind to local ports
  if(!isLocal) {
    local_data_port = argv[2];
    local_control_port = argv[3];
    data_sockfd = setupServerSocket(local_data_port);
    control_sockfd = setupServerSocket(local_control_port);
    socklen_t sin_size = sizeof(their_addr);
    data_sockfd = accept(data_sockfd, (struct sockaddr *)&their_addr, &sin_size);
    printf("Accept connection from local node\n");
    if(data_sockfd == -1) {
      perror("accept data channel");
      exit(1);
    }
    control_sockfd = accept(control_sockfd, (struct sockaddr *)&their_addr, &sin_size);
    if(control_sockfd == -1) {
      perror("accept control channel");
      exit(1);
    }
  }
  //If this node is local, do not need to specify local ports to bind but need to specify the remote host to connect
  else {
    remote_hostname = argv[4];
    remote_data_port = argv[5];
    remote_control_port = argv[6];
    data_sockfd = setupClientSocket(remote_data_port);
    control_sockfd = setupClientSocket(remote_control_port);
    workload_init();
  }
  pthread_t adaptorThread;
  pthread_create(&adaptorThread, 0, adaptor_func, (void*)0);
  transfer_manager_init();
  if(isLocal) {
    transfer_job(INIT_NUM_OF_JOBS / 2);
  }
}

void workload_init() {
  int i;
  for(i = 0; i < WORKSIZE; i++) {
    jobs_head[i] = 1.111111;
  }
  pthread_mutex_lock(&queue_m);
  local_status->queue_length = INIT_NUM_OF_JOBS;
  pthread_mutex_unlock(&queue_m);
}

int setupServerSocket(void* port) {
  int sockfd;
  socklen_t sin_size;
  int yes=1;
  char s[INET6_ADDRSTRLEN];
  int rv;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  if ((rv = getaddrinfo(NULL, (char*)port, &hints, &servinfo)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return 1;
  }
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype,
                         p->ai_protocol)) == -1) {
      perror("server: socket");
      continue;
    }
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
                   sizeof(int)) == -1) {
      perror("setsockopt");
      exit(1);
    }
    if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("server: bind");
      continue;
    }
    break;
  }
  if (p == NULL)  {
    fprintf(stderr, "server: failed to bind\n");
    exit(1);
  }
  printf("Remote node bind to port %s\n", (char*)port);
  freeaddrinfo(servinfo); 
  if (listen(sockfd, BACKLOG) == -1) {
    perror("listen");
    exit(1);
  }
  inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
  return sockfd; 
}

int setupClientSocket(char* port) {
  int sockfd;
  int rv;
  char s[INET6_ADDRSTRLEN];
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if((rv = getaddrinfo(remote_hostname, port, &hints, &servinfo)) != 0){
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    exit(1);
  }
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      perror("client: socket");
      continue;
    }
    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("client: connect");
      continue;
    }
    break;
  }
  
  if (p == NULL) {
    fprintf(stderr, "client: failed to connect\n");
    exit(1);
  }
  printf("Local node: connect to remote node at %s:%s\n", remote_hostname, port);
  inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
  return sockfd;
}

void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

