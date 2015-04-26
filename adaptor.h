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

//Transfer strategy, return 1 if decide to transfer
void decideTransfer();
void* work_func(void* unusedParam);
void move_head();
//Calculate estimated completion time
double calculateECT();
void* adaptor_func(void* unusedParam);
