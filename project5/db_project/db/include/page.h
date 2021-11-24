#pragma once
#include <stdint.h>
#include <pthread.h>

#define PAGE_SIZE 4096 //4KiB page

typedef uint64_t pagenum_t; //page_number
struct page_t {
    //in-memory page structure
    uint8_t raw_data[PAGE_SIZE];
};

//lock head declaration for lock object 
struct lock_head_t;

//lock object
struct lock_t{
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    lock_t *prev_lock = nullptr;
    lock_t *nxt_lock = nullptr;
    lock_t *nxt_lock_in_trx = nullptr;
    lock_head_t *sentinel = nullptr;
    int64_t record_id;
    int lock_mode = 0;
    int owner_trx_id = 0;
    bool sleeping_flag = false;
};

//lock head object
struct lock_head_t{
    int64_t table_id;
    pagenum_t page_id;
    lock_t *head = nullptr;
    lock_t *tail = nullptr;
};