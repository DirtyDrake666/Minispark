#ifndef METRICS_H
#define METRICS_H

#include <stdio.h>
#include "minispark.h" // for TaskMetric

void metrics_submit(TaskMetric* metric);
void metrics_shutdown();
void* metrics_monitor_main(void* arg);

#endif
