#pragma once

#include <stdio.h>
#include "bpt.h"

//Open existing data file using 'pathname' or create one if not existed.
//The return value that indicates the table id should be ‘NUM’. (e.g., a data file with the name “DATA3” should have a table id of 3)
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
//Perform recovery within this function, after the initialization phase. (DBMS initialization --> Analysis Redo Undo)
//Log file will be made using log_path
//Log message file will be made using logmsg_path
//flag should be provided for the recovery test, use 0 for normal recovery protocol, 1 for REDO CRASH, 2 for UNDO CRASH.
//log_num is needed for REDO/UNDO CRASH, the function must return 0 after processing the provided number of logs.
//If success, return 0. Otherwise, return non zero value.
int init_db(int num_buf, int flag, int log_num, char *log_path, char *logmsg_path);

int init_db(int num_buf = DEFAULT_BUFFER_SIZE);

//Shutdown your database management system
//If success, return 0. Otherwise, return non zero value.
int shutdown_db();

//Read a value in the table with a matching key for the transaction having trx_id
//return 0 (SUCCESS): operation is successfully done, and the transaction can continue the next operation.
//return non zero (FAILED): operation is failed (e.g., deadlock detected), and the transaction should be
//aborted. Note that all tasks that need to be handled should be completed in db_find
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size, int trx_id);

//Find the matching key and modify the values
//If found matching key, update the value of the record to values string with its new_val_size and store its size in old_val_size
//return 0 (SUCCESS): operation is successfully done, and the transaction can continue the next operation.
//return non zero (FAILED): operation is failed (e.g., deadlock detected), and the transaction should be aborted.
//Note that all tasks that need to be handled should be completed in db_update
int db_update(int64_t table_id, int64_t key, char *values, uint16_t new_val_size, uint16_t *old_val_size, int trx_id);

//Allocate a transaction structure and initialize it.
//Return a unique transaction id (>= 1) if success, otherwise return 0.
int trx_begin(void);

//Clean up the transaction with the given trx_id (transaction id) and its related information that has
//been used in your lock manager
//Shrinking phase of strict 2PL
//Users can get a response once all modifications of the transaction are flushed from the log buffer to a log file.
//If the user receives a triumphant return, it means that your database can recover committed transactions even after a system crash.
//Return the completed transaction id if success, otherwise return 0.
int trx_commit(int trx_id);

//All modifications should be canceled and be returned to the old state.
//Return the aborted transaction id if successful; otherwise, return 0.
int trx_abort(int trx_id);