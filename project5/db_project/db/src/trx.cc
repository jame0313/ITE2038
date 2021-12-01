#include "trx.h"

namespace TM{
    //maintain trx id length
    int GLOBAL_TXN_ID_LENGTH = 0;

    //trx manager latch
    pthread_mutex_t transaction_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //hash table that mapping trx object in the trx table
    //search key is trx id
    //value is trx object
    std::unordered_map<int, trx_t> trx_table;

    bool is_trx_valid(int trx_id){
        //find given trx in table
        return TM::trx_table.find(trx_id)!=TM::trx_table.end();
    }

    lock_t* find_first_lock_in_trx_table(int trx_id){
        //find given trx in table
        if(TM::trx_table.find(trx_id)!=TM::trx_table.end()){
            //found case
            //return nxt lock in trx list
            return TM::trx_table[trx_id].nxt_lock_in_trx;
        }
        else{
            //not found case
            //return NULL
            return nullptr;
        }
    }

    lock_t* find_last_lock_in_trx_table(int trx_id){
        //find given trx in table
        if(TM::trx_table.find(trx_id)!=TM::trx_table.end()){
            //found case
            //return nxt lock in trx list
            return TM::trx_table[trx_id].last_lock_in_trx;
        }
        else{
            //not found case
            //return NULL
            return nullptr;
        }
    }

    int make_new_txn_id(){
        //increase global txn id for unique txn id
        return ++TM::GLOBAL_TXN_ID_LENGTH;
    }

    void insert_new_trx_in_table(int trx_id){
        //insert default trx object labeled with given trx id
        TM::trx_table.insert(std::make_pair(trx_id, TM::trx_t{std::vector<TM::trx_log_t*>{}, nullptr, nullptr, trx_id}));
        return;
    }

    void erase_trx_in_table(int trx_id){
        //delete trx in table
        TM::trx_table.erase(trx_id);
        return;
    }

    void append_lock_in_table(int trx_id, lock_t* lock_obj){
        if(TM::trx_table[trx_id].last_lock_in_trx && TM::trx_table[trx_id].last_lock_in_trx->waiting_num > 0){
            lock_obj->nxt_lock_in_trx = TM::trx_table[trx_id].nxt_lock_in_trx;
            TM::trx_table[trx_id].nxt_lock_in_trx = lock_obj;
            return;
        }
        if(TM::trx_table[trx_id].last_lock_in_trx){
            //there is lock in the trx list
            //connect with last lock in old list
            TM::trx_table[trx_id].last_lock_in_trx->nxt_lock_in_trx = lock_obj;
        }
        else{
            //there is no lock in the trx list
            //set given object as first lock
            TM::trx_table[trx_id].nxt_lock_in_trx = lock_obj;
        }
        //set given object as last lock
        TM::trx_table[trx_id].last_lock_in_trx = lock_obj;
        return;
    }

    TM::trx_log_t* make_trx_log(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, char *new_values, uint16_t new_val_size, char *old_values, uint16_t old_val_size){
        //make new object
        TM::trx_log_t* ret = new trx_log_t;
        
        //initialize object
        ret->table_id = table_id;
        ret->page_id = page_id;
        ret->key = key;
        ret->slot_number = slot_number;
        ret->old_size = old_val_size;
        ret->new_size = new_val_size;

        //make new string space and copy contents
        ret->old_value = new char[old_val_size];
        ret->new_value = new char[new_val_size];
        memcpy(ret->old_value, old_values, old_val_size);
        memcpy(ret->new_value, new_values, new_val_size);

        //return object
        return ret;
    }

    void append_log_in_list(int trx_id, TM::trx_log_t* log_obj){
        //append log object at the end of list
        TM::trx_table[trx_id].trx_log.push_back(log_obj);
        return;
    }

    void remove_trx_log(int trx_id){
        //get trx log given txn id
        auto& v = TM::trx_table[trx_id].trx_log;
        page_t* tmp_page = new page_t;
        for(auto log : v){
            //release implicit lock
            idx_get_trx_id_in_slot(log->table_id, log->page_id,log->slot_number, tmp_page);
            idx_set_trx_id_in_slot(log->table_id, log->page_id,log->slot_number, 0, tmp_page);
            //delete char string and log object
            delete[] log->old_value;
            delete[] log->new_value;
            delete log;
        }

        v.clear(); //clear list
    }

    void release_all_locks_in_trx(int trx_id){
        //first lock in the trx list
        lock_t *cnt_lock = TM::trx_table[trx_id].nxt_lock_in_trx;

        //release all locks in trx list
        lock_release_all(cnt_lock);

        return;
    }

    void rollback_trx_log(int trx_id){
        //get trx log given txn id
        auto& v = TM::trx_table[trx_id].trx_log;

        //rollback in reverse order
        for(auto it = v.rbegin(); it != v.rend() ; ++it){
            TM::trx_log_t *e = *it; //get trx_log object pointer
            //rollback effect
            idx_update_by_key(e->table_id, e->key, e->old_value, e->old_size, &(e->new_size));
        }
    }
}

int init_trx_manager(void){

    TM::GLOBAL_TXN_ID_LENGTH = 0; //reset trx id pool

    TM::transaction_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //initailize trx table
    TM::trx_table.clear();

    return 0;
}

int trx_begin_txn(void){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    int ret_id = TM::make_new_txn_id(); //get new id

    //make new entry in trx table
    TM::insert_new_trx_in_table(ret_id);

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    return ret_id;
}

int trx_commit_txn(int trx_id){
    int status_code; //check pthread error

    //start critical section
    status_code = lock_acquire_lock_manager_latch(); //get lock manager latch first (avoid deadlock)
    status_code |= pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    //check current trx is valid
    if(TM::is_trx_valid(trx_id)){
        //release phase
        TM::release_all_locks_in_trx(trx_id);

        //delete entry
        TM::remove_trx_log(trx_id);
        TM::erase_trx_in_table(trx_id);
    }
    else{
        //not valid case (error)
        trx_id = 0;
    }

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    status_code |= lock_release_lock_manager_latch();
    if(status_code) return 0; //error

    return trx_id;
}

int trx_abort_txn(int trx_id){
    int status_code; //check pthread error

    //start critical section
    status_code = lock_acquire_lock_manager_latch(); //get lock manager latch first (avoid deadlock)
    status_code |= pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    //check current trx is valid
    if(TM::is_trx_valid(trx_id)){
        TM::rollback_trx_log(trx_id); //rollback effects
        TM::release_all_locks_in_trx(trx_id); //release all lock

        //delete entry
        TM::remove_trx_log(trx_id);
        TM::erase_trx_in_table(trx_id);
    }
    else{
        //not valid case (error)
        trx_id = 0;
    }

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    status_code |= lock_release_lock_manager_latch();
    if(status_code) return 0; //error
    
    return trx_id;
}

int trx_append_lock_in_trx_list(int trx_id, lock_t* lock_obj){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    //check current trx is valid
    if(TM::is_trx_valid(trx_id)){
        //append lock in the list
        TM::append_lock_in_table(trx_id,lock_obj);
    }
    else{
        //not valid case (error)
        trx_id = 0;
    }

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error
    
    return trx_id;
}

int trx_add_log(int64_t table_id, pagenum_t page_id, int64_t key, uint32_t slot_number, char *new_values, uint16_t new_val_size, char *old_values, uint16_t old_val_size, int trx_id){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error

    //check current trx is valid
    if(TM::is_trx_valid(trx_id)){
        //make new log object
        TM::trx_log_t *log_obj = TM::make_trx_log(table_id, page_id, key, slot_number, new_values, new_val_size, old_values, old_val_size);
        //append log in the list
        TM::append_log_in_list(trx_id, log_obj);
    }
    else{
        //not valid case (error)
        trx_id = 0;
    }

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return 0; //error
    
    return trx_id;
}

lock_t* trx_get_last_lock_in_trx_list(int trx_id){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return nullptr; //error

    //tail lock in the list
    lock_t *tail = TM::find_last_lock_in_trx_table(trx_id);
    
    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return nullptr; //error
    
    return tail;
}

int trx_is_this_trx_valid(int trx_id){
    int status_code; //check pthread error

    int ret_val = 0;

    //start critical section
    status_code = pthread_mutex_lock(&TM::transaction_manager_latch);
    if(status_code) return -1; //error

    //check validation
    ret_val = TM::is_trx_valid(trx_id);

    //end critical section
    status_code = pthread_mutex_unlock(&TM::transaction_manager_latch);
    if(status_code) return -1; //error
    
    return ret_val;

}

void close_trx_manager(){
    //start critical section
    lock_acquire_lock_manager_latch(); //get lock manager latch first (avoid deadlock)
    pthread_mutex_lock(&TM::transaction_manager_latch);

    for(auto& it : TM::trx_table){
        int trx_id = it.second.trx_id; //get trx id
        TM::rollback_trx_log(trx_id); //rollback uncommited result
        TM::release_all_locks_in_trx(trx_id); //release all locks
        TM::remove_trx_log(trx_id); //remove log
    }

    //destroy trx table
    TM::trx_table.clear();

    //end critical section
    pthread_mutex_unlock(&TM::transaction_manager_latch);
    lock_release_lock_manager_latch();
    return;
}
