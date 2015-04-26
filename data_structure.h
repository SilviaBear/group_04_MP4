#ifndef DATA_STRUCTURE_H
#define DATA_STRUCTURE_H
typedef struct _status_info {
  double trottling_value;
  double cpu_usage;
  int queue_length;
  long time_per_job;
} status_info;
#endif
