#include <stdio.h> 
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include "monitor.h"
#include "state_manager.h"
#include "data_structure.h"
#include "transfer_manager.h"
#include "adaptor.h"
/*The size of a job is SIZE_PER_JOB * sizeof(double),
  which is 8 bytes * 8 bits/ bytes * 1024 * 1024 * 32 / 2048 = 1 Mb.
  The sending time of one job (ignore header) is 1Mb / 10 Mbps = 0.1 s
  Consider the ACK of TCP (assume using stop and wait policy),
  RTT = 100 ms + 54 ms * 2 = 208 ms.
  Packet loss rate is rather small and is ignorable in our case.
*/
#define NETWORK_DELAY 208 * 1000

extern long SIZE_PER_JOB;

pthread_cond_t status_update_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t status_update_m = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t queue_m;
extern pthread_cond_t queue_cv;


//The time in ms where worker thread start on working
long initialTime;

//If local node, the requested number of jobs to be transferred from remote node
int request_num = -1;

//Only start decision after local node is stably working, we set it to be after 10 sec
int start_decision = 0;

extern int isLocal;
extern double* queue_head;
extern double* jobs_head;
extern status_info* local_status;
extern status_info* remote_status;
//Calculate estimated completion time
double calculateECT(status_info* status);

void* adaptor_func(void* unusedParam) {
  pthread_t workingThread, monitorThread, remoteStateThread, acceptThread;
  pthread_create(&workingThread, 0, work_func, (void*)0);
  pthread_create(&monitorThread, 0, startMonitor, (void*)0);
  //pthread_create(&remoteStateThread, 0, startStateManager, (void*)0);
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
      if(start_decision) {
        //  printf("Start to decide transfer jobs\n");
        decideTransfer();
      }
    }
  }
  pthread_join(workingThread, NULL);
  pthread_join(monitorThread, NULL);
  //pthread_join(remoteStateThread, NULL);
  pthread_join(acceptThread, NULL);
  return NULL;
}

int getTransferSize(double sender_ECT, double sender_rate, double receiver_ECT, double receiver_rate) {
  return (int)(0.8 * (sender_ECT - receiver_ECT) / (sender_rate + receiver_rate));
}
void decideTransfer() {
  double local_ECT = calculateECT(local_status);
  double remote_ECT = calculateECT(remote_status);
  printf("remote_time_per_job %lu\n", remote_status->time_per_job);
  //Transfer jobs to remote node
  if(local_ECT > remote_ECT && remote_status->cpu_usage < 0.95 && local_status->time_per_job < NETWORK_DELAY) {
    int num_jobs = getTransferSize(local_ECT, local_status->time_per_job, remote_ECT, remote_status->time_per_job);
    printf("Decide to transfer %d job\n", num_jobs);
    transfer_job(num_jobs, 0);
  }
  //Require remote node to transfer
  else if(remote_ECT > local_ECT && local_status->cpu_usage < 0.95 && remote_status->time_per_job < NETWORK_DELAY) {
    request_num = getTransferSize(remote_ECT, remote_status->time_per_job, local_ECT, local_status->time_per_job);
    printf("Decide to request remote to transfer %d job\n", request_num);
  }
}

double calculateECT(status_info* status) {
  return status->queue_length * status->time_per_job;
}

void* work_func(void* unusedParam) {
  printf("Working thread start\n");
  struct timespec sleepFor;
  struct timeval workStart;
  struct timeval workEnd;
  struct timeval start_work;
  while(1) {
    printf("Wait before lock\n");
    pthread_mutex_lock(&queue_m);
    printf("Local queue_length: %d\n", local_status->queue_length);
    while(local_status->queue_length == 0) {
      pthread_cond_wait(&queue_cv, &queue_m);
    }
    pthread_mutex_unlock(&queue_m);
    gettimeofday(&start_work, 0);
    initialTime = start_work.tv_sec * 1000 * 1000 + start_work.tv_usec;
    printf("Get initial working time \n");
    while(local_status->queue_length > 0) {
      gettimeofday(&workStart, 0);
      double* current = queue_head;
      printf("Start working on: %lu\n", isLocal? (queue_head - jobs_head)/SIZE_PER_JOB : (jobs_head - queue_head)/SIZE_PER_JOB);
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
      gettimeofday(&workEnd, 0);
      //thread working time in ms
      long workTime = workEnd.tv_usec - workStart.tv_usec;
      sleepFor.tv_sec = 0;
      //sleep time in ns
      sleepFor.tv_nsec = (long)((1 - local_status->trottling_value)/local_status->trottling_value) * workTime * 1000 * 1000;
      nanosleep(&sleepFor, 0);
    }
    if(local_status->queue_length == 0) {
      break;
    }
  }
  //Remote node gather data trunk back to local node
  if(!isLocal) {
    int num_jobs_finished = (jobs_head - queue_head)/SIZE_PER_JOB;
    transfer_job(num_jobs_finished, 1);
  }
  return NULL;
}

void move_head() {
  if(isLocal) {
    queue_head = queue_head + SIZE_PER_JOB;
  }
  else {
    queue_head = queue_head - SIZE_PER_JOB;
  }
}




