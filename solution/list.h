#ifndef LIST_H
#define LIST_H

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>



typedef struct List{
    void **items;      // 存储元素的数组
    int size;          // 当前已存元素个数
    int capacity;      // 当前分配的空间大小
    int iter_index;    // 用于迭代
} List;

// 初始化
List* list_init();

// 添加元素
void list_append(List* list, void* item);

// 获取第 i 个元素
void* list_get(List* list, int index);

// 获取元素个数
int list_size(List* list);

// 释放内存
void list_free(List* list);

// 用于迭代的函数
void list_seek_to_start(List* list);
void* list_next(List* list);

#endif