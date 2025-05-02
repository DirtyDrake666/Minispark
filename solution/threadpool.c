#define _POSIX_C_SOURCE 200809L
#include "threadpool.h"
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include "minispark.h"
#include "metrics.h"
// =================== Internal State =====================

typedef struct TaskNode {
    Task* task;
    struct TaskNode* next;
} TaskNode;

typedef struct {
    pthread_t* threads;
    int num_threads;

    // Work queue
    TaskNode* head;
    TaskNode* tail;

    pthread_mutex_t lock;
    pthread_cond_t not_empty;

    // Waiting and shutting down
    int shutdown;
    int tasks_in_progress;
    pthread_cond_t all_done;
} ThreadPool;

static ThreadPool pool;

// =================== Task Queue Functions =====================

static void enqueue_task(Task* task) {
    TaskNode* node = malloc(sizeof(TaskNode));
    node->task = task;
    node->next = NULL;

    if (pool.tail) {
        pool.tail->next = node;
    } else {
        pool.head = node;
    }
    pool.tail = node;
}

static Task* dequeue_task() {
    if (!pool.head) return NULL;
    TaskNode* node = pool.head;
    Task* task = node->task;
    pool.head = node->next;
    if (!pool.head) pool.tail = NULL;
    free(node);
    return task;
}

// =================== Worker Thread =====================

static void* worker_main(void* arg) {
    while (1) {
        pthread_mutex_lock(&pool.lock);
        while (!pool.head && !pool.shutdown) {
            pthread_cond_wait(&pool.not_empty, &pool.lock);
        }

        if (pool.shutdown && !pool.head) {
            pthread_mutex_unlock(&pool.lock);
            break;
        }

        Task* task = dequeue_task();
        pool.tasks_in_progress++;
        pthread_mutex_unlock(&pool.lock);

        // ======== Run the task =========
        
        materialize_partition(task->rdd, task->pnum);
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        task->metric->scheduled = now;
        task->metric->duration = TIME_DIFF_MICROS(task->metric->created, now);

        metrics_submit(task->metric);

        free(task);

        pthread_mutex_lock(&pool.lock);
        pool.tasks_in_progress--;
        if (!pool.head && pool.tasks_in_progress == 0) {
            pthread_cond_signal(&pool.all_done);
        }
        pthread_mutex_unlock(&pool.lock);
    }
    return NULL;
}

// =================== Public API =====================

void thread_pool_init(int numthreads) {
    pool.threads = malloc(sizeof(pthread_t) * numthreads);
    pool.num_threads = numthreads;
    pool.head = pool.tail = NULL;
    pool.shutdown = 0;
    pool.tasks_in_progress = 0;
    pthread_mutex_init(&pool.lock, NULL);
    pthread_cond_init(&pool.not_empty, NULL);
    pthread_cond_init(&pool.all_done, NULL);

    for (int i = 0; i < numthreads; ++i) {
        pthread_create(&pool.threads[i], NULL, worker_main, NULL);
    }
}

void thread_pool_submit(Task* task) {
    pthread_mutex_lock(&pool.lock);
    enqueue_task(task);
    pthread_cond_signal(&pool.not_empty);
    pthread_mutex_unlock(&pool.lock);
}

void thread_pool_wait() {
    pthread_mutex_lock(&pool.lock);
    while (pool.head || pool.tasks_in_progress > 0) {
        pthread_cond_wait(&pool.all_done, &pool.lock);
    }
    pthread_mutex_unlock(&pool.lock);
}

void thread_pool_destroy() {
    pthread_mutex_lock(&pool.lock);
    pool.shutdown = 1;
    pthread_cond_broadcast(&pool.not_empty);
    pthread_mutex_unlock(&pool.lock);

    for (int i = 0; i < pool.num_threads; ++i) {
        pthread_join(pool.threads[i], NULL);
    }
    //sleep for 1 sec
    //sleep(1);
    free(pool.threads);
    pthread_mutex_destroy(&pool.lock);
    pthread_cond_destroy(&pool.not_empty);
    pthread_cond_destroy(&pool.all_done);
}
