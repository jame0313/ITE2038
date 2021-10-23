#include "api.h"

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
    try{
        return FIM::insert_record(table_id,key,value,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size){
    try{
        return FIM::find_record(table_id,key,ret_val,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_delete(int64_t table_id, int64_t key){
    try{
        return FIM::delete_record(table_id,key);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int init_db(int num_buf){
    return 0;
}

int shutdown_db(){
    try{
        file_close_table_file();
        return 0;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}