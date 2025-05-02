#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <sched.h>
#include "minispark.h"
#include "list.h"
#include <stdarg.h>
#include "metrics.h"
#include "threadpool.h" 
#include <time.h>
#include <unistd.h>
// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile.

typedef struct RDDNode {
    RDD* rdd;
    struct RDDNode* next;
} RDDNode;

static RDDNode* all_rdds = NULL;
// metrics thread
pthread_t metrics_thread;

void print_formatted_metric(TaskMetric* metric, FILE* fp) {
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
	  metric->rdd, metric->pnum, metric->rdd->trans,
	  metric->created.tv_sec, metric->created.tv_nsec / 1000,
	  metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
	  metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  //int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    //maxpartitions = max(maxpartitions, dep->numpartitions);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  if (t == MAP || t == FILTER || t == JOIN) {
        rdd->numpartitions = rdd->dependencies[0]->numpartitions;
        rdd->partitions = list_init(rdd->numpartitions);
    } else {
        int numpartitions = va_arg(args, int);
        void* ctx = va_arg(args, void*);
        rdd->numpartitions = numpartitions;
        rdd->partitions = list_init(numpartitions);
        rdd->ctx = ctx;
        va_end(args);
        //rdd->partitions = NULL;  // PARTITIONBY 会单独设置
    }
  //加入全局
  RDDNode* node = malloc(sizeof(RDDNode));
  node->rdd = rdd;
  node->next = all_rdds;
  all_rdds = node;
  rdd->isFileBacked = 0;
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep, numpartitions);
  rdd->partitions = list_init(numpartitions);
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;
  //sleep(1);
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  rdd->partitions = list_init(numfiles);
  rdd->numpartitions = numfiles;
  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(1);
    }
    List* part = list_init(1);      // 创建一个新的 partition 列表
    list_append(part, fp);          // 把 FILE* 放进去
    list_append(rdd->partitions, part); // 加入到 RDD 的 partitions 中
  }

  rdd->numdependencies = 0;
  rdd->trans = MAP;
  rdd->fn = (void *)identity;
  rdd->isFileBacked = 1;
  //printf("RDDFromFiles (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
  return rdd;
}

void execute(RDD* rdd) {
    //printf("[exec] executing RDD %p (trans = %d)\n", rdd, rdd->trans);

    for (int i = 0; i < rdd->numdependencies; i++) {
        execute(rdd->dependencies[i]);
    }
    thread_pool_wait();
    int numparts = rdd->numpartitions;
    if (rdd->trans == PARTITIONBY) {
        // 针对 PARTITIONBY，不为每个分区创建任务，而是在当前线程中顺序处理所有分区
        for (int i = 0; i < numparts; i++) {
            // 创建 metric
            TaskMetric* metric = malloc(sizeof(TaskMetric));
            clock_gettime(CLOCK_REALTIME, &metric->created);
            metric->rdd = rdd;
            metric->pnum = i;

            // 执行 materialize_partition
            materialize_partition(rdd, i);

            // 记录结束时间，计算持续时间
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            metric->scheduled = now;
            metric->duration = TIME_DIFF_MICROS(metric->created, now);

            // 提交 metric 到 metrics 队列
            metrics_submit(metric);
        }
        return;
    }
    for (int i = 0; i < numparts; ++i) {
        Task* task = malloc(sizeof(Task));
        task->rdd = rdd;
        task->pnum = i;

        TaskMetric* metric = malloc(sizeof(TaskMetric));
        clock_gettime(CLOCK_REALTIME, &metric->created);
        metric->rdd = rdd;
        metric->pnum = i;
        task->metric = metric;

        thread_pool_submit(task);
    }
}


void MS_Run() {
    // 获取当前进程可用的 CPU 核心数
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        perror("sched_getaffinity");
        exit(1);
    }
    int cores = CPU_COUNT(&set);

    // 初始化线程池
    thread_pool_init(cores);

    // 启动 metrics monitor 线程
    pthread_create(&metrics_thread, NULL, metrics_monitor_main, NULL);
}

void MS_TearDown() {
    // 1. 通知 metrics monitor 停止 + 等待其退出
    metrics_shutdown();
    pthread_join(metrics_thread, NULL);

    // 2. 销毁线程池
    thread_pool_destroy();

    // 3. 释放所有 RDDs 和 partition 列表
    RDDNode* cur = all_rdds;
    while (cur) {
        RDD* rdd = cur->rdd;

        if (rdd->partitions) {
            list_free(rdd->partitions);
        }

        free(rdd);
        RDDNode* tmp = cur;
        cur = cur->next;
        free(tmp);
    }

    all_rdds = NULL;  // 清空记录
}


int count(RDD* dataset) {
    int numparts = dataset->numpartitions;

    // 提交每个 partition 的 materialization 任务
    execute(dataset);

    // 等待所有任务完成
    thread_pool_wait();

    // 遍历所有 partition，统计元素个数
    int total = 0;
    for (int i = 0; i < numparts; ++i) {
        List* part = dataset->partitions->items[i];
        if (part != NULL) {
            total += list_size(part);
        }
    }

    return total;
}

void print(RDD* dataset, Printer p) {
    
    // 提交每个 partition 的 materialization 任务
    execute(dataset);
    int numparts = dataset->numpartitions;
    // 等待所有 partition 完成
    thread_pool_wait();

    // 遍历所有 partition 的元素，调用 Printer 打印
    for (int i = 0; i < numparts; ++i) {
        List* part = dataset->partitions->items[i];
        if (part == NULL) continue;

        list_seek_to_start(part);
        void* item;
        while ((item = list_next(part)) != NULL) {
            p(item);
        }
    }
    fflush(stdout);  // 确保输出被刷新
}



void materialize_partition(RDD* rdd, int pnum) {
    assert(rdd->partitions->items != NULL);
    if (rdd->partitions->items[pnum] != NULL) return;

    List* output = list_init(4);

    switch (rdd->trans) {
        case MAP: {
          List* input = rdd->numdependencies > 0
              ? rdd->dependencies[0]->partitions->items[pnum]
              : rdd->partitions->items[pnum];

          if (rdd->fn == (void*)identity) {
              rdd->partitions->items[pnum] = input;
              return;
          }

          Mapper fn = (Mapper)(rdd->fn);
          if (input == NULL) {
            printf("[DEBUG] MAP input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
          }
          assert(input!=NULL);
          list_seek_to_start(input);
          void* item;
          if (rdd->dependencies[0]->isFileBacked) {
            //printf("File Backed RDD is (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
            // FILE_BACKED 情况（如 RDDFromFiles）
            // 这个时候 item 是 FILE*
            while ((item = list_next(input)) != NULL) {
                void* result;
                while ((result = fn(item)) != NULL) {
                    list_append(output, result);
                }
                fclose((FILE*)item);  // 关闭文件
            }

          } else {
            // 普通 mapper，比如 SplitCols，只调用一次
            while ((item = list_next(input)) != NULL) {
                void* result = fn(item);
                if (result != NULL) {
                    list_append(output, result);
                }
            }
        }
          rdd->partitions->items[pnum] = output;
          break;
      }


        case FILTER: {
            List* input = rdd->dependencies[0]->partitions->items[pnum];

            Filter fn = (Filter)(rdd->fn);
            if (input == NULL) {
            printf("[DEBUG] FILTER input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
        }
            assert(input!=NULL);
            list_seek_to_start(input);
            void* item;
            while ((item = list_next(input)) != NULL) {
                int keep = fn(item, rdd->ctx);
                if (keep) {
                    list_append(output, item);
                }
                //else {
                //    free(item);
                //}
            }
            break;
        }

        case PARTITIONBY: {
            Partitioner fn = (Partitioner)(rdd->fn);

            int nparts = rdd->numpartitions;


            if (rdd->partitions->items[pnum] == NULL) {
                rdd->partitions->items[pnum] = list_init(4);
            }

            List* outlist = rdd->partitions->items[pnum];
            for (int i = 0; i < rdd->dependencies[0]->numpartitions; ++i) {
                List* input = rdd->dependencies[0]->partitions->items[i];
                assert(input!=NULL);
                list_seek_to_start(input);
                void* item;
                while ((item = list_next(input)) != NULL) {
                    unsigned long which = fn(item, nparts, rdd->ctx);
                    if (which == (unsigned long)pnum) {
                        list_append(outlist, item);
                    }
                    //list_free(item);   
                }
            }

            return;
        }

        case JOIN: {
            Joiner fn = (Joiner)(rdd->fn);
            materialize_partition(rdd->dependencies[0], pnum);
            materialize_partition(rdd->dependencies[1], pnum);

            List* left = rdd->dependencies[0]->partitions->items[pnum];
            List* right = rdd->dependencies[1]->partitions->items[pnum];
            if (left == NULL) {
            printf("[DEBUG] JOIN left input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
        }
            assert(left!=NULL);
            list_seek_to_start(left);
            void* litem;
            while ((litem = list_next(left)) != NULL) {
                if (right == NULL) {
            printf("[DEBUG] JOIN right input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
        }
                assert(right!=NULL);
                list_seek_to_start(right);
                void* ritem;
                while ((ritem = list_next(right)) != NULL) {
                    void* result = fn(litem, ritem, rdd->ctx);
                    if (result) list_append(output, result);
                }
            }

            // free join inputs
            if (left == NULL) {
            printf("[DEBUG] JOIN left input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
        }
            assert(left!=NULL);
            list_seek_to_start(left);
            void* tmp;
            //while ((tmp = list_next(left)) != NULL) free(tmp);
            //list_free(left);
            if (right == NULL) {
            printf("[DEBUG] JOIN right input is NULL (rdd: %p, trans: %d)\n", (void*)rdd, rdd->trans);
        }
            assert(right!=NULL);
            list_seek_to_start(right);
            //while ((tmp = list_next(right)) != NULL) free(tmp);
            //list_free(right);

            break;
        }

        default:
            fprintf(stderr, "Unknown transform: %d\n", rdd->trans);
            exit(1);
    }

    rdd->partitions->items[pnum] = output;
}