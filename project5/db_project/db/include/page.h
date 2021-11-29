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

//lock object structure
struct lock_t{
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER; //cond var for sleeping
    lock_t *prev_lock = nullptr; //previous lock in page lock list
    lock_t *nxt_lock = nullptr; //next lock in page lock list
    lock_t *nxt_lock_in_trx = nullptr; //next lock in trx lock list
    lock_head_t *sentinel = nullptr; //lock header in lock list
    int64_t record_id; //record id that lock refer to
    int lock_mode = 0; //lock mode
    int owner_trx_id = 0; //trx id which try to acquire this lock 
    int waiting_num = 0; //the number of conflicting lock (mark the lock is sleeping or not)
};

//lock header object structure
struct lock_head_t{
    int64_t table_id;
    pagenum_t page_id;
    lock_t *head = nullptr; //first lock in the list
    lock_t *tail = nullptr; //last lock in the list
};