#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>
#include "data_structure.h"
#include "transfer_manager.h"

extern pthread_cond_t status_update_cv;
extern pthread_mutex_t status_update_m;
extern status_info* local_status;
extern status_info* remote_status;
extern int control_sockfd;
extern int accept_control_sockfd;
extern int isLocal;
extern struct sockaddr_storage their_addr;
extern struct addrinfo hints, *servinfo, *p;
extern socklen_t addr_len;
extern int request_num;
int transfer_sockfd;
//Interval for update current status to remote node in s
long send_interval = 5;

void* transfer_job(int num, int isFinished);

void state_manager_init() {
  transfer_sockfd = isLocal? control_sockfd : accept_control_sockfd;
}

void* listenOnControl(void* unusedParam) {
  int numbytes;
  char recvBuf[256];
  while(1) {
    memset(recvBuf, 0, 256);
    if((numbytes = recvfrom(transfer_sockfd, recvBuf, sizeof(status_info), 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
      perror("control channel recvfrom");
      continue;
    }
    if(strstr(recvBuf, "TRANSFER")) {
      int transfer_size = ntohs(*((int*)(recvBuf + 9)));
      transfer_job(transfer_size, 0);
    }
    else {
      memcpy(remote_status, recvBuf, sizeof(status_info));
      //printf("Current remote state: trottling_value %fl, cpu_usage %fl, queue_length %d, time_per_job %lu\n", remote_status->trottling_value, remote_status->cpu_usage, remote_status->queue_length, remote_status->time_per_job); 
    }
    pthread_cond_broadcast(&status_update_cv);
  }
}

void* sendToControl(void* unusedParam) {
  int numbytes;
  struct timespec sleepFor;
  sleepFor.tv_sec = send_interval;
  sleepFor.tv_nsec = 0;
  while(1) {
    if(request_num > 0) {
      char* sendBuf = (char*)malloc(sizeof(int) + 8);
      memcpy(sendBuf, "TRANSFER", 8);
      int send_num = htons(request_num);
      memcpy(sendBuf + 8, &send_num, sizeof(int));
      if((numbytes = sendto(transfer_sockfd, sendBuf, sizeof(int) + 8, 0, p->ai_addr, p->ai_addrlen)) == -1) {
        perror("sendto");
        continue;
      }
      free(sendBuf);
      request_num = 0;
      printf("Initial transfer request to remote node.\n");
    }
    //printf("Current State: trottling_value %fl, cpu_usage %fl, queue_length %d, time_per_job %lu\n", local_status->trottling_value, local_status->cpu_usage, local_status->queue_length, local_status->time_per_job);
    if((numbytes = sendto(transfer_sockfd, local_status, sizeof(status_info), 0, p->ai_addr, p->ai_addrlen)) == -1) {
      perror("sendto");
      continue;
    }
    nanosleep(&sleepFor, 0);
  }
}

void* startStateManager(void* unusedParam) {
  if(isLocal) {
    transfer_sockfd = control_sockfd;
  }
  else {
    transfer_sockfd = accept_control_sockfd;
  }
  pthread_t sendingThread;
  pthread_create(&sendingThread, 0, sendToControl, (void*)0);
  if(isLocal) {
    pthread_t listeningThread;
    pthread_create(&listeningThread, 0, listenOnControl, (void*)0);
    pthread_join(listeningThread, NULL);
  }
  pthread_join(sendingThread, NULL);
  return NULL;
}


