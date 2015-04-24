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

#DEFINE WORKSIZE 1024 * 1024 * 32

double* workload_init(); 
void data_transmission(void* work);
void receiveFromRemote(void* sockfd);

//Hostname of remote node for data transmission
char* remote_hostname;
//Port of remote node for data transmission
char* remote_data_port;
//Port of remote node for status update
char* remote_control_port;

int main(int argc, char *argv[]) {
  if(argc != 7) {
    printf("Usage: local_data_port, local_control_port, remote_hostname, remote_data_port, remote_control_port, isLocal\n");
    exit(1);
  }
  double* total_work = workload_init();
  
  remote_hostname = argv[1];
  remote_data_port = argv[2];
  remote_control_port = argv[3];
  pthread_t transmission_thread;
  pthread_create(&transmission_thread, 0, data_transmission, (void*)(total_work + WORKSIZE / 2));
  create_job(total_work);
}

double* workload_init() {
  double* work = (double*)malloc(WORK_SIZE * sizeof(double));
  int i;
  for(i = 0; i < WORK_SIZE; i++) {
    work[i] = 1.111111;
  }
  return work;
}

void* data_transmission(void* work) {
  int sockfd, numbytes;
  struct addrinfo hints, *servinfo, *p;
  struct sockaddr_storage their_addr;
  socklen_t addr_len;
  int rv;
  char s[INET6_ADDRSTRLEN];
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if((rv = getaddrinfo(remote_hostname, remote_data_port, &hints, &servinfo)) != 0){
    fprintf(stderr, "data_channel: getaddrinfo: %s\n", gai_strerror(rv));
    return;
  }
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype,
                         p->ai_protocol)) == -1) {
      perror("data_channel: socket");
      continue;
    }

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      perror("data_channel: connect");
      continue;
    }

    break;
  }
  if (p == NULL) {
    fprintf(stderr, "data_channel: failed to connect\n");
    return;
  }

  if ((numbytes = sendto(sockfd, work, WORKSIZE / 2 * sizeof(double), 0, p->ai_addr, p->ai_addrlen)) == -1) {
    perror("data_channel: initial transfer of work failed\n");
    exit(1);
  }

  printf("Local node transfer initial workload to remote host: %s port: %s bytes sent: %d\n", remote_hostname, remote_data_port, numbytes);

  pthread_t receive_thread;
  pthread_create(&receive_thread, 0, receiveFromRemote, (void*)(&sockfd));
  pthread_join(receive_thread, NULL);
}

void receiveFromRemote(void* socket_fd) {
  int sockfd = *((int*)socket_fd);
}
