#pragma once

#include <stdio.h>
#include "file.h"
#include "bpt.h"

//Open existing data file using 'pathname' or create one if not existed.
//If success, return the unique table id, which represents the own table in this database.
//Otherwise, return negative value.
int64_t open_table(char *pathname);

//Insert input record with its size to data file at the right place.
//If success, return 0. Otherwise, return non zero value.
int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size);

//Find the record containing input key.
//If found matching key, store matched value string in ret_val and matched size in val_size.
//If success, return 0. Otherwise, return non zero value.
//The caller should allocate memory for a record structure
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size);

//Find the matching record and delete it if found.
//If success, return 0. Otherwise, return non zero value.
int db_delete(int64_t table_id, int64_t key);

//Initialize database management system.
//If success, return 0. Otherwise, return non zero value.
int init_db(int num_buf = 1024);

//Shutdown your database management system
//If success, return 0. Otherwise, return non zero value.
int shutdown_db();