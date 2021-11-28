#pragma once
#include "lock.h"
#include "bpt.h"
#include <vector>

//Initialize any data structures required for implementing a trx manager, such as a trx table, a trx manager latch, etc.
//If success, return 0. Otherwise, return a non zero value.
int init_trx_manager(void);

//Allocate a transaction structure and initialize it.
//Return a unique transaction id (>= 1) if success, otherwise return 0.
int trx_begin_txn(void);

//Clean up the transaction with the given trx_id (transaction id) and its related information that has
//been used in your lock manager
//Shrinking phase of strict 2PL
//Return the completed transaction id if success, otherwise return 0.
int trx_commit_txn(int trx_id);

//Clean up the transaction with the given trx_id (transaction id) and its related information that has
//been used in your lock manager
//Rollback all effect by current transaction and release all locks
//Return the aborted transaction id if success, otherwise return 0.
int trx_abort_txn(int trx_id);

//Append given lock object to given trx list in transaction table
//Return the transaction id if success, otherwise return 0.
int trx_append_lock_in_trx_list(int trx_id, lock_t* lock_obj);

//Append log to trx log list
//which means old_value changes into new_value
//Return the transaction id if success, otherwise return 0.
int trx_add_log(int64_t table_id, int64_t key, char *new_values, uint16_t new_val_size, char *old_values, uint16_t old_val_size, int trx_id);

//get last lock object in given trx list
//Return lock's pointer if success, otherwise return null.
lock_t* trx_get_last_lock_in_trx_list(int trx_id);

//Rollback all uncommitted result and destroy trx table
void close_trx_manager();

//inner struct and function used in TransactionManager
namespace TM{

    //transaction log structure
    //record's value changes old_value to new_value
    struct trx_log_t{
        char *old_value; //point to old value string
        char *new_value; //point to new value string
        int64_t table_id; 
        int64_t key;
        uint16_t old_size; //old value string length
        uint16_t new_size; //new value string length
    };

    //transaction table entry structure
    struct trx_t{
        std::vector<trx_log_t*> trx_log; //log list of modification in current trx
        lock_t* nxt_lock_in_trx; //next(head) lock of trx list
        lock_t* last_lock_in_trx; //last(tail) lock of trx list
        int trx_id; //identifier of entry
    };

    //check given trx id is valid
    //by finding corresponding entry in table
    bool is_trx_valid(int trx_id);

    //get first lock in lock list
    //of given trx_id's trx table entry
    //return lock object pointer or nullptr if not found
    lock_t* find_first_lock_in_trx_table(int trx_id);

    //get last lock in lock list
    //of given trx_id's trx table entry
    //return lock object pointer or nullptr if not found
    lock_t* find_last_lock_in_trx_table(int trx_id);

    //get new id for new transaction in trx begin
    //return new id
    int make_new_txn_id();

    //insert new trx entry in transaction table at the given id position
    void insert_new_trx_in_table(int trx_id);

    //delete trx entry in transaction table
    void erase_trx_in_table(int trx_id);

    //append lock object in trx lock list
    //of given trx_id's trx table entry
    void append_lock_in_table(int trx_id, lock_t* lock_obj);

    //make and initialize trx_log object using given info
    //return new trx_log object's pointer
    TM::trx_log_t* make_trx_log(int64_t table_id, int64_t key, char *new_values, uint16_t new_val_size, char *old_values, uint16_t old_val_size);
    
    //append given trx_log object into given trx's log list
    void append_log_in_list(int trx_id, TM::trx_log_t* log_obj);

    //destroy given transaction's log
    void remove_trx_log(int trx_id);

    //release all locks in trx lock list
    //of given trx_id's trx table entry
    void release_all_locks_in_trx(int trx_id);

    //rollback all effects made by given trx
    //by using log list in corresponding trx table entry
    void rollback_trx_log(int trx_id);
}