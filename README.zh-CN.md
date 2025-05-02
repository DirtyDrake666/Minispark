[🇨🇳 中文版 README](./README.zh-CN.md) | [🇬🇧 English README](./README.md)

## MiniSpark：多线程数据处理框架

**概述：**  
开发了一个简化版的多线程数据处理框架，灵感源自 Apache Spark，专注于高效的数据处理流水线和基于线程的并行计算。

**主要特性：**

* **有向无环图（DAG）表示：**  
  * 实现了数据转换和动作（如 map、filter、join、partition），通过节点和边组织在 DAG 中。  
  * 启用了延迟计算，仅在执行终止操作（count、print）时才进行数据物化，提高效率。

* **基于线程的并行性：**  
  * 构建了线程池基础架构，根据硬件并发性动态利用核心数。  
  * 实现了对独立 DAG 分段和 RDD 分区的并行物化，加速处理速度。

* **同步与并发控制：**  
  * 使用锁和条件变量维护线程安全的任务队列并确保同步。  
  * 通过精心设计的任务调度避免死锁，确保任务在依赖项解决后才执行。

* **性能与指标监控：**  
  * 集成了性能监控，记录执行时间和任务统计信息，并将日志写入外部指标文件用于分析。

**使用技术：**  
C 编程、POSIX 线程、同步原语（锁、条件变量）、Linux 环境

**成果：**  
交付了一个稳健的原型，展示了高效的并行计算和数据管理，通过了模拟真实大数据工作负载的广泛正确性和性能测试。

---

**项目结构：**

* `applications/`：示例应用程序，展示 MiniSpark 用法  
* `lib/`：核心库函数和工具  
* `tests/`：单元测试和集成测试  
* `solution/`：主要实现源码文件

```
MiniSpark_Project/
├── applications/
│   └── 示例应用和用例
├── lib/
│   ├── lib.c
│   └── lib.h
├── tests/
│   └── 自动化测试脚本（全部通过）
├── solution/
│   ├── list.c
│   ├── list.h
│   ├── metrics.c
│   ├── metrics.h
│   ├── minispark.c (主要实现)
│   ├── minispark.h (接口定义)
│   ├── threadpool.c
│   └── threadpool.h
└── Makefile
```