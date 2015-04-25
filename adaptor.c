#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include "monitor.h"
#include "state_manager.h"
#include "data_structure.h"
#include "transfer_manager.h"

/*The size of a job is SIZE_PER_JOB * sizeof(double),
  which is 8 bytes * 8 bits/ bytes * 1024 * 1024 * 32 / 2048 = 1 Mb.
  The sending time of one job (ignore header) is 1Mb / 10 Mbps = 0.1 s
  Consider the ACK of TCP (assume using stop and wait policy),
  RTT = 100 ms + 54 ms * 2 = 208 ms.
  Packet loss rate is rather small and is ignorable in our case.
*/
#define NETWORK_DELAY 208

extern SIZE_PER_JOB;

pthread_cond_t status_update_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t status_update_m = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t queue_m;
extern pthread_cond_t queue_cv;

status_info* local_status;
status_info* remote_status;

extern int isLocal;
extern double* queue_head;
//Transfer strategy, return 1 if decide to transfer
void decideTransfer();
void* work_func(void* unusedParam);
void* startMonitor(void* unusedParam);
void* startStateManager(void* unusedParam);
void* accept_job(void* unusedParam);
void move_head();
//Calculate estimated completion time
double calculateECT();

void adaptor_func(void* unusedParam) {
  local_status = (status_info*)malloc(sizeof(status_info));
  remote_status = (status_info*)malloc(sizeof(status_info));
  pthread_t workingThread, monitorThread, remoteStateThread, acceptThread;
  pthread_create(&workingThread, 0, work_func, (void*)0);
  pthread_create(&monitorThread, 0, startMonitor, (void*)0);
  pthread_create(&remoteStateThread, 0, startStateManager, (void*)0);
  pthread_create(&acceptThread, 0, accept_job, (void*)0);
//Local node decide the transfer strategy
  if(isLocal) {
    while(1) {
      int sleep = 1;
      pthread_mutex_lock(&status_update_m);
      while(sleep) {
        pthread_cond_wait(&status_update_cv, &status_update_m);
        sleep = 0;
      }
      pthread_mutex_unlock(&status_update_m);
      decideTransfer();
    }
  }
  pthread_join(workingThread, NULL);
  pthread_join(monitorThread, NULL);
  pthread_join(remoteStateThread, NULL);
  pthread_join(acceptThread, NULL);
}

void decideTransfer() {
  double local_ECT = calculateECT(local_status);
  double remote_ECT = calculateECT(remote_status);
  
}

double calculateECT(status_info* status) {
  return status->queue_length / status->trottling_value / status->cpu_usage;
}

void transferBack() {
}

void* work_func(void* unusedParam) {
  struct timespec sleepFor;
  struct timeval workStart;
  struct timeval workEnd;
  while(1) {
    pthread_mutex_lock(&queue_m);
    while(local_status->queue_length == 0) {
      pthread_cond_wait(&queue_cv, &queue_m);
    }
    pthread_mutex_unlock(&queue_m);
    while(local_status->queue_length > 0) {
      gettimeofday(&workStart);
      double* current = queue_head;
      int i;
      int j;
      for(i = 0; i < SIZE_PER_JOB; i++) {
	for(j = 0; j < 1000; j++) {
	  *current += 1.111111;
	}
	current++;
      }
      pthread_mutex_lock(&queue_m);
      move_head();
      local_status->queue_length--;
      pthread_mutex_unlock(&queue_m);
      gettimeofday(&workEnd);
      //thread working time in ms
      long workTime = workEnd.tv_usec - workStart.tv_usec;
      sleepFor.tv_sec = 0;
      //sleep time in ns
      sleepFor.tv_nsec = local_status->trottling_value * workTime * 1000 * 1000;
      nanosleep(&sleepFor, 0);
    }
  }
  transferBack();
}

void move_head() {
  if(isLocal) {
    queue_head = queue_head + SIZE_PER_JOB;
  }
  else {
    queue_head = queue_head - SIZE_PER_JOB;
  }
}




