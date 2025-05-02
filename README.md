[🇨🇳 中文版 README](./README.zh-CN.md) | [🇬🇧 English README](./README.md)

## MiniSpark: Multi-threaded Data Processing Framework

**Overview:**
Developed a simplified, multi-threaded data processing framework inspired by Apache Spark, emphasizing efficient data processing pipelines and parallel computing using threads.

**Key Features:**

* **Directed Acyclic Graph (DAG) Representation:**

  * Implemented data transformations and actions (e.g., map, filter, join, partition) organized as nodes and edges within a DAG.
  * Enabled deferred computation for efficiency, materializing data only upon executing terminal actions (count, print).

* **Thread-based Parallelism:**

  * Built thread pool infrastructure with dynamic core utilization based on hardware concurrency.
  * Achieved parallel materialization of independent DAG segments and RDD partitions to optimize processing speed.

* **Synchronization and Concurrency Control:**

  * Applied locks and condition variables to maintain thread-safe task queues and ensure synchronization.
  * Avoided deadlocks through careful task scheduling, ensuring tasks were executed only after dependencies were resolved.

* **Performance and Metrics:**

  * Integrated performance monitoring by capturing execution times and task statistics, logging to an external metrics file for profiling.

**Technologies Used:**
C Programming, POSIX Threads, Synchronization Primitives (Locks, Condition Variables), Linux Environment

**Outcome:**
Delivered a robust prototype demonstrating efficient parallel computation and data management, passing extensive correctness and performance tests designed to simulate realistic big data workloads.

---

**Project Structure:**

* `applications/`: Example applications demonstrating MiniSpark usage.
* `lib/`: Core library functions and utilities.
* `tests/`: Unit tests and integration tests.
* `solution/`: Main implementation source files.

```
MiniSpark_Project/
├── applications/
│   └── Example applications and use-cases
├── lib/
│   ├── lib.c
│   └── lib.h
├── tests/
│   └── Automated testing scripts
├── solution/
│   ├── list.c
│   ├── list.h
│   ├── metrics.c
│   ├── metrics.h
│   ├── minispark.c (main implementation)
│   ├── minispark.h (interface definitions)
│   ├── threadpool.c
│   └── threadpool.h
└── Makefile (build automation)
```
