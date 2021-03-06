#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <algorithm>
#include "buffer.h"

#define DEFAULT_ORDER 124


#define PAGE_HEADER_SIZE 128 //128bytes page header
#define MAX_SLOT_NUMBER 64 // max # of slot
#define MIN_VALUE_SIZE 50 //min size of value
#define MAX_VALUE_SIZE 112 // max size of value
#define MAX_KEY_NUMBER DEFAULT_ORDER*2 //max number of keys in internal page
#define MAX_FREE_SPACE 2500 //max free space in leaf page

//Insert input record with its size to data file at the right place.
//If success, return 0. Otherwise, return non zero value.
int idx_insert_by_key(int64_t table_id, int64_t key, char *value, uint16_t val_size);

//Find the record containing input key.
//If found matching key, store matched value string in ret_val and matched size in val_size.
//If success, return 0. Otherwise, return non zero value.
//The caller should allocate memory for a record structure
int idx_find_by_key(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size);

//Find the matching record and delete it if found.
//If success, return 0. Otherwise, return non zero value.
int idx_delete_by_key(int64_t table_id, int64_t key);

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

    //get page from BM and return new page number
    pagenum_t make_page(int64_t table_id);
    
    //change root page number in header page
    //you can set root page number to 0 when del_tree_flag is on
    //return 0 if success or -1 if fail
    int change_root_page(int64_t table_id, pagenum_t root_page_number, bool del_tree_flag = false);

    //find the leaf page in which given key is likely to be
    //return 0 if there is no tree
    //throw msg in looping situation (doesn't has key but not leaf)
    pagenum_t find_leaf_page(int64_t table_id, int64_t key);

    //find the record value with given key
    //save record valud in ret_val(caller must provide it) and set size in val_size
    //you can get existence state by using key only and setting ret_val and val_size null
    //return 0 if success or -1 if fail
    int find_record(int64_t table_id, int64_t key, char *ret_val = NULL, uint16_t* val_size = NULL);
    
    //insert master function
    //insert record in tree
    //return 0 if success or -1 if failed
    //if given key is already in tree, return -1
    int insert_record(int64_t table_id, int64_t key, char *value, uint16_t val_size);
    
    //make root page and put first record
    //set initial state in root
    //return new root page number
    pagenum_t init_new_tree(int64_t table_id, int64_t key, char *value, uint16_t val_size);   
    
    //insert record in given leaf page
    //push key in sorted order and push value in right next free space (packed)
    void insert_into_leaf_page(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size);
    
    //make new pages and split records in leaf page and new record into two pages evenly
    //Set the first record that is equal to or greater than 50% of the total size
    //on the Page Body as the point to split, and then move that record, and all records
    //after that to the new leaf page.
    //set new leaf page at right of old leaf page
    //need push new key(new leaf's first key) and new leaf page in parent's page
    //return insert_into_parent_page call(next phase)
    int insert_into_leaf_page_after_splitting(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size);
    
    //insert key and right page number in parent page
    //return 0 if success
    int insert_into_parent_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    
    //make new root page and set initial state
    //put left page #, key, right page # in new root page
    //connect two given pages to new root page as parent
    //return new root page number
    pagenum_t insert_into_new_root_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    
    //insert key and right page number in given page
    //push key and page # in sorted order
    void insert_into_page(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    
    //make new pages
    //split key and page number in page and new data (key and right page #) into two pages evenly
    //old page has DEFAULT_ORDER keys and new page has DEFAULT_ORDER keys as a result
    //set new page at right of old page
    //need push middle key and new page in parent's page
    //return insert_into_parent_page call 
    int insert_into_page_after_splitting(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number);
    
    //delete master function
    //delete key and corresponding value in tree
    //return 0 if success or -1 if fail
    //if there is no such key, return -1
    int delete_record(int64_t table_id, int64_t key);

    //delete key(and corresponding data(page number or value)) and in page
    //and make tree obey key occupancy invariant
    //return 0 if success or -1 if failed
    int delete_entry(pagenum_t page_number, int64_t table_id, int64_t key);
    
    //remove key and data in page
    //data is value(leaf page) or page number(internal page)
    //fill gap caused by deleting key
    void remove_entry_from_page(pagenum_t page_number, uint64_t table_id, int64_t key);
    
    //deal with root page changes
    //use child as root or delete tree when root page is empty
    //return existed root page number or 0 if entire tree is deleted
    //return new root page number if root page is changed
    pagenum_t adjust_root_page(pagenum_t root_page_number, int64_t table_id);
    
    //move all page's contents to neighbor page
    //to merge two pages into one page
    //need delete right page number in parent page
    //return delete_entry call
    int merge_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost);
    
    //move some page's contents to neighbor page
    //pull a record from neighbor page until its free space becomes
    //smaller than the threshold 
    void redistribute_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost);

}