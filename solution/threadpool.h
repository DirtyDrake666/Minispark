#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "minispark.h"  // for Task
#include <pthread.h>

// 初始化线程池，传入线程数量
void thread_pool_init(int numthreads);


// 销毁线程池，回收资源
void thread_pool_destroy();
// 阻塞直到所有任务都完成
void thread_pool_wait();
// 提交一个任务给线程池执行
void thread_pool_submit(Task* task);


#endif
