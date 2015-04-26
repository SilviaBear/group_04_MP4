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
#include "huffman.h"
#include "data_structure.h"

extern long SIZE_PER_JOB;
extern int data_sockfd;
extern int accept_data_sockfd;
extern int isLocal;
//Data structre for remote socket if it is local node
extern struct sockaddr_storage their_addr;
//Data structure for local socket information if it is remote node
extern struct addrinfo hints, *servinfo, *p;
extern socklen_t addr_len;

extern double* queue_head;
extern double* jobs_head;
extern status_info* local_status;

extern pthread_mutex_t queue_m;
extern pthread_mutex_t status_update_m;
extern pthread_cond_t queue_cv;

extern int shouldUpdate;
extern int remote_finished;

int state_sockfd;
void* accept_job(void* unusedParam);

void transfer_manager_init() {
  state_sockfd = isLocal? data_sockfd : accept_data_sockfd;
}

double* find_end();

void* transfer_job(int num, int isFinished) {
  if(num > local_status->queue_length && !isFinished) {
    printf("Do no need to transfer request %d queue_length %d\n", num, local_status->queue_length);
    return NULL;
  }
  pthread_mutex_lock(&queue_m);
  addr_len = sizeof(their_addr);
  int numbytes;
  int send_num = htons(num);
  char sendBuf[256];
  memset(sendBuf, 0, 256);
  memcpy(sendBuf, "START:", 7);
  memcpy(sendBuf + 7, &send_num, sizeof(int));
  //Send sentinel packet for job transfer start
  if((numbytes = sendto(state_sockfd, sendBuf, 7 + sizeof(int), 0, p->ai_addr, p->ai_addrlen)) == -1) {
    perror("sendto");
    exit(1);
  }
  printf("Send sentinel bits for request of %d \n", num);

  if(isFinished) {
    int total_finished_jobs = (jobs_head - queue_head) / SIZE_PER_JOB;
    int i;
    unsigned char* origin_buf = (unsigned char*)(jobs_head - i * SIZE_PER_JOB);
    unsigned char* compressed_buf;
    uint32_t len = 0;
    huffman_encode_memory(origin_buf, SIZE_PER_JOB * sizeof(double), &compressed_buf, &len);
    printf("Initial length: %d compressed length: %d\n", SIZE_PER_JOB * sizeof(double), len);
    for(i = 0; i < total_finished_jobs; i++) {
      if((numbytes = sendto(state_sockfd, compressed_buf, len, 0, p->ai_addr, p->ai_addrlen)) == -1) {
        perror("transfer_job sendto");
        exit(1);
      } 
    }
    printf("Remote node transfer all finished jobs back.\n");
    return NULL;
  }
  else {
    double* queue_end = find_end();
    int i;
    //Send each job trunk
    for(i = 0; i < num; i++) {
      //If local node, transfer the jobs from last significant bit
      if(isLocal) {
	unsigned char* origin_buf = (unsigned char*)(queue_end - i * SIZE_PER_JOB);
	unsigned char* compressed_buf;
	uint32_t len = 0;
	huffman_encode_memory(origin_buf, SIZE_PER_JOB * sizeof(double), &compressed_buf, &len);
    
	if((numbytes = sendto(state_sockfd, compressed_buf, len, 0, p->ai_addr, p->ai_addrlen)) == -1) {
	  perror("transfer_job sendto");
	  exit(1);
	}
      }
      //If remote node, transfer the jobs from most significant bit
      else {
	unsigned char* origin_buf = (unsigned char*)(queue_end + i * SIZE_PER_JOB);
	unsigned char* compressed_buf;
	uint32_t len = 0;
	huffman_encode_memory(origin_buf, SIZE_PER_JOB * sizeof(double), &compressed_buf, &len);    
	if((numbytes = sendto(state_sockfd, compressed_buf, len, 0, p->ai_addr, p->ai_addrlen)) == -1) {
	  perror("sendto");
	  exit(1);
	}
      }
    }
  //Decrease the local work queue length
  local_status->queue_length -= num;
  pthread_mutex_unlock(&queue_m);
  printf("Transfer work: %d jobs, after transfer local queue_length %d \n", num, local_status->queue_length);
  }
  return NULL;
}

void* accept_job(void* unusedParam) {
  int numbytes;
  char* recvBuf = (char*)malloc(256 * sizeof(char));
  while(1) {
    if((numbytes = recvfrom(state_sockfd, recvBuf, 256, 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
      perror("accept_job sentinel recvfrom");
      exit(1);
    }

    if(strstr(recvBuf, "START")) {
      int recv_num = *((int*)(recvBuf + 7));
      int num = ntohs(recv_num);
      printf("Accept job, size: %d\n", num);
      int i;
      pthread_mutex_lock(&queue_m);
      double* end_of_queue = find_end();
      for(i = 0; i < num; i++) {
        //For local node, put new jobs after the least significant bit of current job
        if(isLocal) {
	  unsigned char* recvBuf = (char*)malloc(SIZE_PER_JOB * sizeof(double));
          if((numbytes = recvfrom(state_sockfd, recvBuf, SIZE_PER_JOB * sizeof(double), 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
            perror("accept_job recvfrom");
            exit(1);
          }
	  uint32_t len = 0;
	  unsigned char* origin_buf = (unsigned_char*)(end_of_queue + i * SIZE_PER_JOB);
	  uint32_t ori_len;
	  huffman_decode_memory(recvBuf, numbytes, &origin_buf, &ori_len);
        }
        //For remote node, put the new jobs before the most significant bit of current job list
        else{
	  unsigned char* recvBuf = (char*)malloc(SIZE_PER_JOB * sizeof(double));
          if((numbytes = recvfrom(state_sockfd, end_of_queue - i * SIZE_PER_JOB, SIZE_PER_JOB * sizeof(double), 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
            perror("recvfrom");
            exit(1);
          }
	  uint32_t len = 0;
	  unsigned char* origin_buf = (unsigned_char*)(end_of_queue - i * SIZE_PER_JOB);
	  uint32_t ori_len;
	  huffman_decode_memory(recvBuf, numbytes, &origin_buf, &ori_len);
        }
      }
      local_status->queue_length += num;
      pthread_mutex_unlock(&queue_m);
      pthread_cond_broadcast(&queue_cv);
    }
    //Must be local node
    else if(strstr(recvBuf, "ENDING")) {
      int recv_num = *((int*)(recvBuf + 7));
      int num = ntohs(recv_num);
      printf("Finally gather data from remote node, size: %d\n", num);
      int i;
      pthread_mutex_lock(&queue_m);
      double* end_of_queue = find_end();
      for(i = 0; i < num; i++) {
        if((numbytes = recvfrom(state_sockfd, end_of_queue + i * SIZE_PER_JOB, SIZE_PER_JOB * sizeof(double), 0, (struct sockaddr *)&their_addr, &addr_len)) == -1) {
            perror("accept_job recvfrom");
            exit(1);
          }
      }
      pthread_mutex_unlock(&queue_m);
    }
  }
}

double* find_end() {
  if(isLocal) {
    return queue_head + local_status->queue_length * SIZE_PER_JOB;
  }
  else {
    return queue_head - local_status->queue_length * SIZE_PER_JOB;
  }
}

