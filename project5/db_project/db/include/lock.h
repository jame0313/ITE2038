#pragma once

#include "page.h"
#include "trx.h"
#include <pthread.h>
#include <unordered_map>
#include <utility>

#define SHARED_LOCK_MODE 0
#define EXCLUSIVE_LOCK_MODE 1

typedef std::pair<int64_t, pagenum_t> page_id;

//Initialize any data structures required for implementing a lock table, such as a hash table, a lock table latch, etc.
//If success, return 0. Otherwise, return a non zero value.
int init_lock_table(void);

//Allocate and append a new lock object to the lock list of the record having the page id and the key.
//If there is a predecessor’s conflicting lock object in the lock list, sleep until the predecessor releases its lock.
//If there is no predecessor’s conflicting lock object, return the address of the new lock object.
//If an error occurs, return NULL.
//lock_mode : 0 (SHARED) or 1 (EXCLUSIVE)
lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);

//Remove the lock_obj from the lock list.
//If there is a successor’s lock waiting for the transaction releasing the lock, wake up the successor.
//If success, return 0. Otherwise, return a non zero value.
int lock_release(lock_t* lock_obj);

//Destroy lock table
void close_lock_table();

//inner struct and function used in LockManager
namespace LM{
    //inner structure for hashing pair object
    //hash algorithm used in boost lib + std::hash
    // https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
    struct hash_pair {
        template <class T1, class T2>
        size_t operator()(const std::pair<T1, T2>& p) const;
    };

    //find lock head object in lock table
    //where it's pagenum and table id is same with given parameter
    //return object's pointer or null if not found
    lock_head_t* find_lock_head_in_table(int64_t table_id, pagenum_t pagenum);
    
    //insert lock head object into lock table
    //at the position where it's pagenum and table id is same with given parameter
    //return new lock head object's pointer inserted
    lock_head_t* insert_new_lock_head_in_table(int64_t table_id, pagenum_t pagenum);

    //detect deadlock will occur when given lock inserted into lock table
    //determine deadlock when lock object is owned by source trx (check cycle)
    //If there is no deadlock, return 1 if there is conflict lock or 0 if not
    //If there is deadlock, return -1
    //source_trx_id should be first lock object's owner trx id 
    //DO NOT SET is_first flag false at the first time(always return -1)
    int detect_deadlock(lock_t* lock_obj, int source_trx_id, bool is_first = true);
}