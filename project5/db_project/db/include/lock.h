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
//If there is no predecessor’s conflicting lock object, return 0.
//If an error occurs, return -1.
//lock_mode : 0 (SHARED) or 1 (EXCLUSIVE)
int lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, int trx_id, int lock_mode);

//Remove the lock_obj from the lock list.
//If there is a successor’s lock waiting for the transaction releasing the lock, wake up the successor.
//If success, return 0. Otherwise, return a non zero value.
int lock_release(lock_t* lock_obj);

//Remove the all lock_obj in trx list from the lock list.
//NO lock manager latch lock in this API
//YOU SHOULD LOCK BEFORE AND UNLOCK AFTER
//If success, return 0. Otherwise, return a non zero value.
int lock_release_all(lock_t* lock_obj);

//acquire global lock manager latch
//If success, return 0. Otherwise, return a non zero value.
int lock_acquire_lock_manager_latch();

//release global lock manager latch
//If success, return 0. Otherwise, return a non zero value.
int lock_release_lock_manager_latch();

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

    //make new lock object
    //and initialize lock object with given parameter
    //return new lock object pointer
    lock_t* make_and_init_new_lock_object(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, int trx_id, int lock_mode);

    //connect given lock object with lock header
    //if there is no corresponding lock header, make new one
    //return 0 if success, or non-zero if not
    int connect_with_lock_head(int64_t table_id, pagenum_t page_id, lock_t* lock_obj);
    
    //find lock head object in lock table
    //where it's pagenum and table id is same with given parameter
    //return object's pointer or null if not found
    lock_head_t* find_lock_head_in_table(int64_t table_id, pagenum_t pagenum);
    
    //insert lock head object into lock table
    //at the position where it's pagenum and table id is same with given parameter
    //return new lock head object's pointer inserted
    lock_head_t* insert_new_lock_head_in_table(int64_t table_id, pagenum_t pagenum);

    //append lock in the lock list in object's sentinel
    //also append in the trx lock list
    void append_lock_in_lock_list(lock_t* lock_obj);

    //remove lock in the lock list in object's sentinel
    void remove_lock_from_lock_list(lock_t* lock_obj);

    //find lock object that compatible with given lock object
    //in page lock list defined by lock head
    //return object's pointer or null if not found 
    lock_t* find_compatible_lock_in_lock_list(lock_head_t* lock_head, lock_t* lock_obj);

    //find lock object already acquired by given txn for lock compression
    //in page lock list defined by lock head
    //return object's pointer or null if not found 
    lock_t* find_same_trx_lock_in_lock_list(lock_head_t* lock_head, int trx_id);

    //detect deadlock will occur when given lock inserted into lock table
    //determine deadlock when lock object is owned by source trx (check cycle)
    //If there is no deadlock, return the number of conflicting lock
    //If there is deadlock, return -1
    //source_trx_id should be first lock object's owner trx id 
    //DO NOT SET is_first flag false at the first time(always return -1)
    int detect_deadlock(lock_t* lock_obj, int source_trx_id, bool is_first = true);

    //find and wake up the successor
    //if it is waiting for the current transaction releasing the lock
    //and no conflicting transaction left after releasing this lock
    void wake_conflicting_lock_in_lock_list(lock_t* lock_obj);

    //convert implicit lock to explicit lock
    //which makes new explicit lock object
    //and append into page lock list and trx list
    void convert_implicit_lock_to_explicit_lock(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, int trx_id);

    //try to do lock compression
    //do lock compression where there is lock belonged to same trx
    //remove lock obj when lock compression occurred
    void try_to_lock_compression(lock_t* lock_obj);
    
    //Allocate and append a new lock object to the lock list of the record having the page id and the key.
    //If there is a predecessor’s conflicting lock object in the lock list, sleep until the predecessor releases its lock.
    //If there is no predecessor’s conflicting lock object, return 0.
    //If an error occurs, return -1
    int try_to_acquire_lock_object(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, int trx_id, int lock_mode);

    //Remove the lock_obj from the lock list.
    //If there is a successor’s lock waiting for the transaction releasing the lock, wake up the successor.
    //If success, return 0. Otherwise, return a non zero value.
    int release_acquired_lock(lock_t* lock_obj);
}
