#include "list.h"
#include <stdlib.h>


List* list_init(int capacity) {
    List* list = malloc(sizeof(List));
    list->items = malloc(sizeof(void*) * capacity);
    for (int i = 0; i < capacity; i++) {
        list->items[i] = NULL;
    }
    list->size = 0;
    list->capacity = capacity;
    list->iter_index = 0;
    return list;
}
void list_append(List* list, void* item) {
    if (list->size >= list->capacity) {
        list->capacity *= 2;
        list->items = realloc(list->items, sizeof(void*) * list->capacity);
    }
    list->items[list->size++] = item;
}

void* list_get(List* list, int index) {
    if (index < 0 || index >= list->size) {
        return NULL;
    }
    return list->items[index];
}
int list_size(List* list) {
    return list->size;
}
void list_free(List* list) {
    for (int i = 0; i < list->size; i++) {
        free(list->items[i]);
    }
    free(list->items);
    free(list);
}

void list_seek_to_start(List* list) {
    list->iter_index = 0;
}

void* list_next(List* list) {
    if (list->iter_index >= list->size) {
        return NULL;
    }
    return list->items[list->iter_index++];
}
