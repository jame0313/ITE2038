#include "lock.h"

namespace LM{
    //lock manager latch
    pthread_mutex_t lock_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //hash table that mapping page lock list in the table
    //search key is page_id({table_id, pagenum})
    //value is lock header's pointer
    std::unordered_map<page_id, lock_head_t*, LM::hash_pair> lock_table;

    //code by boost lib
    // https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
    template <class T1, class T2>
    size_t LM::hash_pair::operator()(const std::pair<T1, T2>& p) const {
        auto hash1 = std::hash<T1>{}(p.first);
        auto hash2 = std::hash<T2>{}(p.second);
        //use magic number and some bit shift to fit hash feature
        return hash1 ^ hash2 + 0x9e3779b9 + (hash2<<6) + (hash2>>2);
    }

    lock_head_t* find_lock_head_in_table(int64_t table_id, pagenum_t pagenum){
        page_id pid = {table_id, pagenum}; //make page_id to use as search key in hash table
        if(LM::lock_table.find(pid)!=LM::lock_table.end()){
            //found case
            //return corresponding object pointer
            return LM::lock_table[pid];
        }
        else{
            //not found case
            //return null
            return nullptr;
        }
    }

    lock_head_t* insert_new_lock_head_in_table(int64_t table_id, pagenum_t pagenum){
        //make and initialize new lock head
        lock_head_t* lock_head = new lock_head_t{table_id,pagenum,nullptr,nullptr};
        page_id pid = {table_id, pagenum}; //make page_id to use as search key in hash table
        //insert lock head at pid
        LM::lock_table.insert(std::make_pair(pid,lock_head));
        return lock_head;
    }

    lock_t* find_compatible_lock_in_lock_list(lock_head_t* lock_head, lock_t* lock_obj){
        //find lock already acquired by given txn
        //found lock should be X lock or given lock should be S lock

        //start at head lock
        lock_t *cnt_lock = lock_head->head;

        //current lock object's info
        //use as distinguish compatible lock in page lock list
        int64_t key = lock_obj->record_id;
        int trx_id = lock_obj->owner_trx_id;
        int lock_mode = lock_obj->lock_mode;

        //searching phase
        while(cnt_lock){
            //find same record lock
            //which trx id same and can share with this lock
            if(cnt_lock->record_id == key && cnt_lock->owner_trx_id == trx_id && ((cnt_lock->lock_mode == EXCLUSIVE_LOCK_MODE) || (lock_mode == SHARED_LOCK_MODE))){
                //can share with this lock
                return cnt_lock;
            }
            //get next lock
            cnt_lock = cnt_lock->nxt_lock;
        }
        return nullptr; //not found
    }

    int detect_deadlock(lock_t* lock_obj, int source_trx_id, bool is_first){
        if(!lock_obj){
            //current lock object is NULL
            //no lock in current trx
            //no conflict here
            return 0;
        }

        if(!is_first){
            //not first step
            //source transaction wait for current transaction
            if(lock_obj->owner_trx_id == source_trx_id){
                //current is actually source trx
                //cycle will occur in lock table
                //deadlock detected
                return -1;
            }
            if(lock_obj->waiting_num == 0){
                //current transaction doesn't wait for anything
                //no out-degree edge in current trx
                //no conflict here
                return 0;
            }
        }

        //find out-degree edge from current lock object in wait-for graph
        //current trx may be conflicted with one X lock or several S locks
        //find first conflicting X lock or a series of S locks

        //current lock object's info
        //use as distinguish conflicting lock in page lock list
        int64_t key = lock_obj->record_id;
        int trx_id = lock_obj->owner_trx_id;
        int lock_mode = lock_obj->lock_mode;

        //start at right before current lock
        lock_t* cnt_lock = lock_obj->prev_lock;

        //flags for filter first conflicting lock
        int waiting_num = 0;
        bool has_prev_shared_lock = false;
        bool has_prev_exclusive_lock = false;

        //searching phase
        while(cnt_lock){
            //find same record lock
            //which trx id is not same (same trx lock is not conflicted)
            //and at least one is X lock (only S lock doesn't make conflict)
            if(cnt_lock->record_id == key && cnt_lock->owner_trx_id != trx_id && (cnt_lock->lock_mode | lock_mode) == EXCLUSIVE_LOCK_MODE){
                if(cnt_lock->lock_mode == EXCLUSIVE_LOCK_MODE){
                    //current lock is X lock
                    has_prev_exclusive_lock = true;
                    if(has_prev_shared_lock){
                        //there is conflicting S lock after this lock
                        //no need to check further (we already checked all S lock)
                        break;
                    }
                }
                else{
                    //current lock is S lock
                    has_prev_shared_lock = true;
                }

                //current trx waits for cnt_lock's trx(cnt_lock->owner_trx_id)
                
                //count conflicting trx
                waiting_num ++;

                //get last lock in next step trx
                lock_t* nxt_lock_obj = trx_get_last_lock_in_trx_list(cnt_lock->owner_trx_id);
                
                //check deadlock in next step trx
                int ret = LM::detect_deadlock(nxt_lock_obj, source_trx_id, false);
                if(ret == -1){
                    //deadlock occured
                    //return -1
                    return ret;
                }
                
                if(has_prev_exclusive_lock){
                    //checked X lock in deadlock detection
                    //no need to check further
                    break;
                }
            }

            //get previous lock
            cnt_lock = cnt_lock->prev_lock;
        }

        //return the number of conflicting operation
        return waiting_num;
    }

    void wake_conflicting_lock_in_lock_list(lock_t* lock_obj){
        //find in-degree edge from current lock object in wait-for graph
        //current trx may be conflicted with one X lock or several S locks
        //find first conflicting X lock or a series of S locks

        //current lock object's info
        //use as distinguish conflicting lock in page lock list
        int64_t key = lock_obj->record_id;
        int trx_id = lock_obj->owner_trx_id;
        int lock_mode = lock_obj->lock_mode;

        //start at right next to current lock
        lock_t *cnt_lock = lock_obj->nxt_lock;

        //flags for filter first conflicting lock
        bool has_prev_shared_lock = false;
        bool has_prev_exclusive_lock = false;

        //searching phase
        while(cnt_lock){
            //find same record lock
            //which trx id is not same (same trx lock is not conflicted)
            //and at least one is X lock (only S lock doesn't make conflict)
            if(cnt_lock->record_id == key && cnt_lock->owner_trx_id != trx_id && (cnt_lock->lock_mode | lock_mode) == EXCLUSIVE_LOCK_MODE){
                if(cnt_lock->lock_mode == EXCLUSIVE_LOCK_MODE){
                    //current lock is X lock
                    has_prev_exclusive_lock = true;
                    if(has_prev_shared_lock){
                        //there is conflicting S lock after this lock
                        //no need to check further (we already checked all S lock)
                        break;
                    }
                }
                else{
                    //current lock is S lock
                    has_prev_shared_lock = true;
                }

                if(cnt_lock->waiting_num > 0){
                    //current lock is waiting for this lock's release
                    cnt_lock->waiting_num--;
                    //wake up the successor if there is no longer conflicting trx
                    if(cnt_lock->waiting_num == 0) pthread_cond_signal(&cnt_lock->cond);
                }

                if(has_prev_exclusive_lock){
                    //wake up X lock
                    //no need to check further
                    break;
                }
            }

            //get next lock
            cnt_lock = cnt_lock->nxt_lock;
        }

        return;
    }

    lock_t* try_to_acquire_lock_object(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode){
        //make new lock
        lock_t* ret = new lock_t; 

        //initialize lock object
        ret->lock_mode = lock_mode;
        ret->owner_trx_id = trx_id;
        ret->record_id = key;

        //find page lock list header
        lock_head_t *lock_head = LM::find_lock_head_in_table(table_id,page_id);

        if(!lock_head){
            //no lock header case
            //insert new lock head
            lock_head = LM::insert_new_lock_head_in_table(table_id,page_id);
        }

        if(lock_head->table_id != table_id || lock_head->page_id != page_id){
            //lock table mismatched case
            delete ret;
            return nullptr; //error
        }

        //find compatible lock(acquired previous lock in same txn) in the list
        lock_t* compatible_lock = LM::find_compatible_lock_in_lock_list(lock_head, ret);

        if(compatible_lock){
            //found compatible lock
            //just return this lock
            delete ret;
            return compatible_lock;
        }

        //connect new lock
        ret->prev_lock = lock_head->tail;
        ret->sentinel = lock_head;

        //check deadlock
        int conflicting_flag = LM::detect_deadlock(ret, trx_id);

        if(conflicting_flag == -1){
            //deadlock occurred
            delete ret;
            return nullptr; //error
        }

        if(lock_head->tail){
            //connect next to tail
            lock_head->tail->nxt_lock = ret;
        }
        else{
            //there is no lock in the list
            //current lock should be head
            lock_head->head = ret;
        }

        //set tail lock
        lock_head->tail = ret;

        //make connection in respect to transaction table lock list
        trx_append_lock_in_trx_list(trx_id, ret);

        if(conflicting_flag > 0){
            //there is conflicting operation

            //set waiting num
            ret->waiting_num = conflicting_flag;

            //wait for all conflicting locks released
            while(ret->waiting_num > 0){
                pthread_cond_wait(&ret->cond, &LM::lock_manager_latch);
            }
        }

        return ret; //return acquired lock
    }

    int release_acquired_lock(lock_t* lock_obj){
        //get lock header
        lock_head_t *lock_head = lock_obj->sentinel;

        if(lock_obj->prev_lock){
            //predecessor lock existed
            //connect it with nxt lock
            lock_obj->prev_lock->nxt_lock = lock_obj->nxt_lock;
        }
        else{
            //current lock is first lock in the list
            //update head lock
            lock_head->head = lock_obj->nxt_lock;
        }

        if(lock_obj->nxt_lock){
            //successor lock existed
            //connect it with prev lock
            lock_obj->nxt_lock->prev_lock = lock_obj->prev_lock;
        }
        else{
            //current lock is last lock in the list
            //update tail lock
            lock_head->tail = lock_obj->prev_lock;
        }

        //find conflicting lock and wake it up when there is no longer conflicting trx
        LM::wake_conflicting_lock_in_lock_list(lock_obj);

        //delete lock object
        delete lock_obj;

        return 0; //success
    }

}

int init_lock_table(void){

    LM::lock_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //initailize lock table
    LM::lock_table.clear();

    return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&LM::lock_manager_latch);
    if(status_code) return nullptr; //error

    //acquire lock
    lock_t* acquired_lock = LM::try_to_acquire_lock_object(table_id, page_id, key, trx_id, lock_mode);
    
    //end critical section
    status_code = pthread_mutex_unlock(&LM::lock_manager_latch);
    if(status_code){
        delete acquired_lock;
        return nullptr; //error
    }

    //return lock object
    return acquired_lock;
}

int lock_release(lock_t* lock_obj){
    int status_code; //check pthread error

    //start critical section
    status_code = pthread_mutex_lock(&LM::lock_manager_latch);
    if(status_code){
        return status_code; //error
    }

    //release lock
    LM::release_acquired_lock(lock_obj);

    //end critical section
    status_code = pthread_mutex_unlock(&LM::lock_manager_latch);
    if(status_code){
        return status_code; //error
    }

    return 0; //success
}

int lock_release_all(lock_t* lock_obj){
    //release all lock in trx list
    while(lock_obj){
        //get next lock in trx list
        lock_t* nxt_lock_obj = lock_obj->nxt_lock_in_trx;
        
        //release current lock
        LM::release_acquired_lock(lock_obj);
        
        lock_obj = nxt_lock_obj; //next step lock
    }

    return 0; //success
}

int lock_acquire_lock_manager_latch(){
    return pthread_mutex_lock(&LM::lock_manager_latch);
}

int lock_release_lock_manager_latch(){
    return pthread_mutex_unlock(&LM::lock_manager_latch);
}

void close_lock_table(){
    //start critical section
    pthread_mutex_lock(&LM::lock_manager_latch);

    for(auto& it : LM::lock_table){
        delete it.second; //delete lock head
    }

    LM::lock_table.clear(); //clear lock table
    
    //end critical section
    pthread_mutex_unlock(&LM::lock_manager_latch);

    return;
}

