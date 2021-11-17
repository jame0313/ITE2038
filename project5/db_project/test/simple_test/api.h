#ifndef DB_API_H_
#define DB_API_H_

#include <stdint.h>

int64_t open_table(char* pathname);

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);

int db_delete(int64_t table_id, int64_t key);

int init_db();

int shutdown_db();

#endif /* DB_API_H */
