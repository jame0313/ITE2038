#pragma once
#include <vector>
#include "lock.h"
#include "bpt.h"

int init_trx_manager(void);

//Allocate a transaction structure and initialize it.
//Return a unique transaction id (>= 1) if success, otherwise return 0.
int trx_begin_txn(void);

//Clean up the transaction with the given trx_id (transaction id) and its related information that has
//been used in your lock manager
//Shrinking phase of strict 2PL
//Return the completed transaction id if success, otherwise return 0.
int trx_commit_txn(int trx_id);

int trx_abort_txn(int trx_id);

int trx_append_lock_in_trx_list(int trx_id, lock_t* lock_obj);

lock_t* trx_get_last_lock_in_trx_list(int trx_id);

void close_trx_manager();

namespace TM{
    struct trx_log_t{
        char *old_value;
        char *new_value;
        int64_t table_id;
        int64_t key;
        uint16_t old_size;
        uint16_t new_size;
    };

    struct trx_t{
        std::vector<trx_log_t> trx_log;
        lock_t* nxt_lock_in_trx;
        lock_t* last_lock_in_trx;
        int trx_id;
    };

    bool is_trx_valid(int trx_id);
    lock_t* find_first_lock_in_trx_table(int trx_id);
    lock_t* find_last_lock_in_trx_table(int trx_id);
    int make_new_txn_id();
    void insert_new_trx_in_table(int trx_id);
    void erase_trx_in_table(int trx_id);
    void append_lock_in_table(int trx_id, lock_t* lock_obj);
    void remove_trx_log(int trx_id);
    void release_all_locks_in_trx(int trx_id);
    void rollback_trx_log(int trx_id);
}