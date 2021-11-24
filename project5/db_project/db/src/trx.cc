#include "trx.h"

namespace TM{
    int GLOBAL_TXN_ID_LENGTH = 0;

    pthread_mutex_t transaction_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //hash table that mapping ctrl block in the list
    //search key is page_id({table_id, pagenum})
    //value is block num
    std::unordered_map<int, trx_t> trx_table;

    bool is_trx_valid(int trx_id){
        return TM::trx_table.find(trx_id)!=TM::trx_table.end();
    }

    lock_t* find_first_lock_in_trx_table(int trx_id){
        if(TM::trx_table.find(trx_id)!=TM::trx_table.end()){
            return TM::trx_table[trx_id].nxt_lock_in_trx;
        }
        else{
            return nullptr;
        }
    }

    lock_t* find_last_lock_in_trx_table(int trx_id){
        if(TM::trx_table.find(trx_id)!=TM::trx_table.end()){
            return TM::trx_table[trx_id].last_lock_in_trx;
        }
        else{
            return nullptr;
        }
    }

    int make_new_txn_id(){
        return ++TM::GLOBAL_TXN_ID_LENGTH;
    }

    void insert_new_trx_in_table(int trx_id){
        TM::trx_table.insert(std::make_pair(trx_id, TM::trx_t{std::vector<TM::trx_log_t>{}, nullptr, nullptr, trx_id}));
        return;
    }

    void erase_trx_in_table(int trx_id){
        TM::trx_table.erase(trx_id);
        return;
    }

    void append_lock_in_table(int trx_id, lock_t* lock_obj){
        if(TM::trx_table[trx_id].last_lock_in_trx){
            TM::trx_table[trx_id].last_lock_in_trx->nxt_lock_in_trx = lock_obj;
        }
        else{
            TM::trx_table[trx_id].nxt_lock_in_trx = lock_obj;
        }
        TM::trx_table[trx_id].last_lock_in_trx = lock_obj;
        return;
    }

    void remove_trx_log(int trx_id){
        auto& v = TM::trx_table[trx_id].trx_log;

        for(auto& log : v){
            delete[] log.old_value;
            delete[] log.new_value;
        }

        v.clear();
    }

    void release_all_locks_in_trx(int trx_id){
        lock_t *cnt_lock = TM::find_first_lock_in_trx_table(trx_id);

        while(cnt_lock){
            lock_t* nxt_lock = cnt_lock->nxt_lock_in_trx;
            lock_release(cnt_lock);
            cnt_lock = nxt_lock;
        }

        return;
    }

    void rollback_trx_log(int trx_id){
        auto v = TM::trx_table[trx_id].trx_log;

        for(auto it = v.rbegin(); it != v.rend() ; ++it){
            //TODO : ROLLBACK
            idx_update_by_key_trx(it->table_id, it->key, it->old_value,it->old_size, &(it->new_size), trx_id);
        }
    }
}

int init_trx_manager(void){
    pthread_mutex_lock(&TM::transaction_manager_latch);

    TM::trx_table.clear();

    pthread_mutex_unlock(&TM::transaction_manager_latch);
    return 0;
}

int trx_begin_txn(void){
    int status_code;

    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    int ret_id = TM::make_new_txn_id();
    TM::insert_new_trx_in_table(ret_id);

    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    return ret_id;
}

int trx_commit_txn(int trx_id){
    int status_code;
    
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    if(TM::is_trx_valid(trx_id)){
        TM::release_all_locks_in_trx(trx_id);
        TM::remove_trx_log(trx_id);
        TM::erase_trx_in_table(trx_id);
    }
    else{
        trx_id = 0;
    }

    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    return trx_id;
}

int trx_abort_txn(int trx_id){
    int status_code;
    
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    if(TM::is_trx_valid(trx_id)){
        TM::rollback_trx_log(trx_id);
        TM::release_all_locks_in_trx(trx_id);
        TM::remove_trx_log(trx_id);
        TM::erase_trx_in_table(trx_id);
    }
    else{
        trx_id = 0;
    }
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0;
    
    return trx_id;
}

int trx_append_lock_in_trx_list(int trx_id, lock_t* lock_obj){
    int status_code;
    
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0;

    if(TM::is_trx_valid(trx_id)){
        TM::append_lock_in_table(trx_id,lock_obj);
    }
    else{
        trx_id = 0;
    }

    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0;
    
    return trx_id;
}

lock_t* trx_get_last_lock_in_trx_list(int trx_id){
    int status_code;
    
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return nullptr;

    lock_t *tail = TM::find_last_lock_in_trx_table(trx_id);
    
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return nullptr;
    
    return tail;
}

void close_trx_manager(){
    pthread_mutex_lock(&TM::transaction_manager_latch);

    for(auto& it : TM::trx_table){
        int trx_id = it.second.trx_id;
        TM::remove_trx_log(trx_id);
        TM::release_all_locks_in_trx(trx_id);
    }

    TM::trx_table.clear();

    pthread_mutex_unlock(&TM::transaction_manager_latch);
    return;
}
