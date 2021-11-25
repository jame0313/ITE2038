#include "api.h"

int init_db(int num_buf){
    return init_buffer(num_buf);
}

int shutdown_db(){
    try{
        close_trx_manager();
        close_lock_table();
        buffer_close_table_file();
        file_close_table_file();
        return 0;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}

int64_t open_table(char *pathname){
    try{
        int64_t tid = file_open_table_file(pathname);
        return tid;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size){
    return idx_insert_by_key(table_id, key, value, val_size);
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size){
    return idx_find_by_key(table_id, key, ret_val, val_size);
}

int db_delete(int64_t table_id, int64_t key){
    return idx_delete_by_key(table_id, key);
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size, int trx_id){
    return idx_find_by_key_trx(table_id, key, ret_val, val_size, trx_id);
}

int db_update(int64_t table_id, int64_t key, char *values, uint16_t new_val_size, uint16_t *old_val_size, int trx_id){
    return idx_update_by_key_trx(table_id, key, values, new_val_size, old_val_size, trx_id);
}

int trx_begin(void){
    return trx_begin_txn();
}

int trx_commit(int trx_id){
    return trx_commit_txn(trx_id);
}

int close_table(int64_t table_id){
    try{

        buffer_close_table_file(table_id);
        file_close_table_file(table_id);
        return 0;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}