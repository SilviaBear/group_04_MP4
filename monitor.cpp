#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pthread.h>
#include "data_structure.h"

//CPU time in microsecond
clock_t last_cpu_time;
//Elapse time in microsecond
long last_total_time;

//Interval for hardware monitor to update in s
long monitor_interval = 2;

extern long initialTime;
extern long SIZE_PER_JOB;
extern int start_decision;
extern pthread_cond_t status_update_cv;
extern pthread_mutex_t status_update_m;
extern status_info* local_status;
extern double* queue_head;
extern double* jobs_head;
extern int isLocal;
struct timeval temp_time;

void getCPUUsage();
void getTimePerJob();
//Function for thread listening to user's real-time input of trottling value
void* listenToUserCommand(void* unusedParam);

void* startMonitor(void* unusedParam) {
  pthread_t inputThread;
  pthread_create(&inputThread, 0, listenToUserCommand, (void*)0);
  struct timespec sleepFor;
  sleepFor.tv_sec = 1;
  sleepFor.tv_nsec = 0;
  while(1) {
    getCPUUsage();
    getTimePerJob();
    nanosleep(&sleepFor, 0);
    pthread_cond_broadcast(&status_update_cv);
  }
}

void getCPUUsage() {
  clock_t current_cpu_time = clock() / CLOCKS_PER_SEC * 1000 * 1000;
  gettimeofday(&temp_time, 0);
  long current_total_time = temp_time.tv_sec * 1000 * 1000 + temp_time.tv_usec; 
  if(!last_cpu_time) {
    local_status->cpu_usage = 0;
  }
  else {
    local_status->cpu_usage = (double)(current_cpu_time - last_cpu_time) / (double)(current_total_time - last_total_time);
  }
  last_cpu_time = current_cpu_time;
  last_total_time = current_total_time;
}

void getTimePerJob() {
  struct timeval t;
  gettimeofday(&t, 0);
  long currentTime = t.tv_sec * 1000 * 1000 + t.tv_usec;
  long jobs_finished =  isLocal? (queue_head - jobs_head) / (long)SIZE_PER_JOB : (jobs_head - queue_head) / (long)SIZE_PER_JOB;
  if(jobs_finished == 0) {
    return;
  }
  printf("current time %lu, initialTime %lu", currentTime, initialTime);
  local_status->time_per_job = (currentTime - initialTime) / (long)jobs_finished;
  printf("local time_per_job: %lu\n", local_status->time_per_job);
}

void* listenToUserCommand(void* unusedParam) {
  char command[100];
  while(read(STDIN_FILENO, command, 100) > 0) {
    local_status->trottling_value = atoi(command);
    pthread_cond_broadcast(&status_update_cv);
  }
  return NULL;
}
