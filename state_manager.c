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
int sockfd;
//Interval for update current status to remote node
long send_interval = 50;

void state_manager_init() {
  sockfd = isLocal? control_sockfd : accept_control_sockfd;
}

void* listenOnControl(void* unusedParam) {
  int numbytes;
  while(1) {
    if((numbytes = recvfrom(sockfd, remote_status, sizeof(status_info), 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
      perror("control channel recvfrom");
      continue;
    }
    pthread_cond_broadcast(&status_update_cv);
  }
}

void* sendToControl(void* unusedParam) {
  int numbytes;
  struct timespec sleepFor;
  sleepFor.tv_sec = 0;
  sleepFor.tv_nsec = send_interval * 1000 * 1000;
  while(1) {
    if((numbytes = sendto(sockfd, local_status, sizeof(status_info), 0, p->ai_addr, p->ai_addrlen)) == -1) {
      perror("sendto");
      continue;
    }
    pthread_cond_broadcast(&status_update_cv);
    nanosleep(&sleepFor, 0);
  }
}

void* startStateManager(void* unusedParam) {
  if(isLocal) {
    sockfd = control_sockfd;
  }
  else {
    sockfd = accept_control_sockfd;
  }
  pthread_t sendingThread;
  pthread_create(&sendingThread, 0, sendToControl, (void*)0);
  if(isLocal) {
    pthread_t listeningThread;
    pthread_create(&listeningThread, 0, listenOnControl, (void*)0);
  }
}


