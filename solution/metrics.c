#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "minispark.h"

#define METRICS_LOG "metrics.log"

typedef struct MetricNode {
    TaskMetric* metric;
    struct MetricNode* next;
} MetricNode;

static MetricNode* head = NULL;
static MetricNode* tail = NULL;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;

static int shutting_down = 0;

// 插入一个 metric
void metrics_submit(TaskMetric* m) {
    //printf("[submit] metric submitted: pnum=%d\n", m->pnum);
    MetricNode* node = malloc(sizeof(MetricNode));
    node->metric = m;
    node->next = NULL;

    pthread_mutex_lock(&lock);
    if (tail) {
        tail->next = node;
    } else {
        head = node;
    }
    tail = node;
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&lock);
}

// 调用时通知 metrics 线程退出（可选）
void metrics_shutdown() {
    pthread_mutex_lock(&lock);
    shutting_down = 1;
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&lock);
}



void* metrics_monitor_main(void* arg) {
    (void)arg;
    FILE* fp = fopen(METRICS_LOG, "w");
    if (!fp) {
        perror("fopen metrics.log");
        return NULL;
    }
    //fprintf(fp, "[metrics] thread started\n");
    while (1) {
        pthread_mutex_lock(&lock);
        while (!head && !shutting_down) {
            pthread_cond_wait(&not_empty, &lock);
        }

        if (shutting_down && !head) {
            pthread_mutex_unlock(&lock);
            break;
        }

        MetricNode* node = head;
        head = head->next;
        if (!head) tail = NULL;

        pthread_mutex_unlock(&lock);

        if (node && node->metric) {
            //fprintf(fp, "[metrics] got a metric, writing...\n");
            print_formatted_metric(node->metric,fp);
            free(node->metric);
            free(node);
        }
    }

    fclose(fp);
    return NULL;
}
