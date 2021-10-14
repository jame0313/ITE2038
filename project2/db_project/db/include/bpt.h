#pragma once

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <algorithm>
#include "page.h"

#define DEFAULT_ORDER 124


#define PAGE_HEADER_SIZE 128 //128bytes page header
#define MAX_SLOT_NUMBER 64 // max # of slot
#define MIN_VALUE_SIZE 50 //min size of value
#define MAX_VALUE_SIZE 112 // max size of value
#define MAX_KEY_NUMBER DEFAULT_ORDER*2 //max number of keys in internal page
#define MAX_FREE_SPACE 2500



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
int init_db();

//Shutdown your database management system
//If success, return 0. Otherwise, return non zero value.
int shutdown_db();



//inner struct and function used in FileandIndexManager
namespace FIM{
    //header page(first page) structure
    struct header_page_t{
        pagenum_t free_page_number; //point to the first free page(head of free page list) or indicate no free page if 0
        uint64_t number_of_pages; //the number of pages paginated in db file
        pagenum_t root_page_number; //pointing the root page within the data file or indicate no root page if 0
        uint8_t __reserved__[PAGE_SIZE - 2*sizeof(pagenum_t) - sizeof(uint64_t)]; //not used for now
    };

    //internal, leaf page header structure
    struct page_header_t{
        pagenum_t parent_page_number; //point to parent page or indicate root if 0
        uint32_t is_leaf; //0 if internal page, 1 if leaf page
        uint32_t number_of_keys; //number of keys within page
    };

    //leaf page's slot structure (find record in page)
    //value range: [offset, offset+size)
    struct page_slot_t{
        int64_t key;
        uint16_t size; //size of value
        uint16_t offset; //in-page offset, point begin of value
    } __attribute__((packed));

    //(key, page(pointer)) pair structure
    struct keypagenum_pair_t{
        int64_t key;
        pagenum_t page_number;
    };

    //leaf page structure
    //value can be store in value_space and later part of slot
    struct leaf_page_t{
        FIM::page_header_t page_header;
        uint8_t __reserved__[PAGE_HEADER_SIZE - sizeof(FIM::page_header_t) - sizeof(uint64_t) - sizeof(pagenum_t)]; //not used for now
        uint64_t amount_of_free_space; //free space in slot and value space
        pagenum_t right_sibling_page_number; //point to right sibling page or indicate rightmost leaf page if 0
        FIM::page_slot_t slot[MAX_SLOT_NUMBER]; //slot list (or some value at end part)
        uint8_t value_space[PAGE_SIZE - PAGE_HEADER_SIZE - MAX_SLOT_NUMBER*sizeof(FIM::page_slot_t)]; //value space to store record value
    };

    //internal page structure
    struct internal_page_t{
        FIM::page_header_t page_header;
        uint8_t __reserved__[PAGE_HEADER_SIZE - sizeof(FIM::page_header_t) - sizeof(pagenum_t)]; //not used for now
        pagenum_t leftmost_page_number; //point to leftmost page
        FIM::keypagenum_pair_t key_and_page[MAX_KEY_NUMBER]; //key and page number
    };
    
    //union all type of page to reinterpret shared bitfield by using each type's member variable
    union _fim_page_t {
        page_t _raw_page;
        FIM::header_page_t _header_page;
        FIM::internal_page_t _internal_page;
        FIM::leaf_page_t _leaf_page;
    };

    pagenum_t find_leaf_page(int64_t table_id, int64_t key);
    int find_record(int64_t table_id, int64_t key, char *ret_val = NULL, uint16_t* val_size = NULL);
    int insert_record(int64_t table_id, int64_t key, char *value, uint16_t val_size);
    pagenum_t init_new_tree(int64_t table_id, int64_t key, char *value, uint16_t val_size);
    pagenum_t make_page(int64_t table_id);
    void insert_into_leaf_page(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size);
    pagenum_t insert_into_leaf_page_after_splitting(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size);
    pagenum_t insert_into_parent_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    pagenum_t insert_into_new_root_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    void insert_into_page(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    pagenum_t insert_into_page_after_splitting(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    int delete_record(int64_t table_id, int64_t key);
    int delete_entry(pagenum_t page_number, int64_t table_id, int64_t key, pagenum_t target_page_number = 0ULL);
    void remove_entry_from_page(pagenum_t page_number, uint64_t table_id, int64_t key, pagenum_t  target_page_number);
    pagenum_t adjust_root_page(pagenum_t root_page_number, int64_t table_id);
    void merge_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost);
    void redistribute_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost);

}