#pragma once

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <algorithm>
#include "page.h"

#define DEFAULT_ORDER 124

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 20

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

#define PAGE_HEADER_SIZE 128 //128bytes page header
#define MAX_SLOT_NUMBER 64 // max # of slot
#define MIN_VALUE_SIZE 50 //min size of value
#define MAX_VALUE_SIZE 112 // max size of value
#define MAX_KEY_NUMBER DEFAULT_ORDER*2 //max number of keys in internal page

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */
typedef struct record {
    int value;
} record;

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
typedef struct node {
    void ** pointers;
    int * keys;
    struct node * parent;
    bool is_leaf;
    int num_keys;
    struct node * next; // Used for queue.
} node;

// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
extern int order;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
extern node * queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;

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

// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice( void );
void print_license( int licence_part );
void usage_1( void );
void usage_2( void );
void usage_3( void );
void enqueue( node * new_node );
node * dequeue( void );
int height( node * root );
int path_to_root( node * root, node * child );
void print_leaves( node * root );
void print_tree( node * root );
void find_and_print(node * root, int key, bool verbose); 
void find_and_print_range(node * root, int range1, int range2, bool verbose); 
int find_range( node * root, int key_start, int key_end, bool verbose,
int returned_keys[], void * returned_pointers[]); 
node * find_leaf( node * root, int key, bool verbose );
record * find( node * root, int key, bool verbose );
int cut( int length );

// Insertion.

record * make_record(int value);
node * make_node( void );
node * make_leaf( void );
int get_left_index(node * parent, node * left);
node * insert_into_leaf( node * leaf, int key, record * pointer );
node * insert_into_leaf_after_splitting(node * root, node * leaf, int key,
                    record * pointer);
node * insert_into_node(node * root, node * parent, 
    int left_index, int key, node * right);
node * insert_into_node_after_splitting(node * root, node * parent,
                    int left_index,
    int key, node * right);
node * insert_into_parent(node * root, node * left, int key, node * right);
node * insert_into_new_root(node * left, int key, node * right);
node * start_new_tree(int key, record * pointer);
node * insert( node * root, int key, int value );

// Deletion.

int get_neighbor_index( node * n );
node * adjust_root(node * root);
node * coalesce_nodes(node * root, node * n, node * neighbor,
              int neighbor_index, int k_prime);
node * redistribute_nodes(node * root, node * n, node * neighbor,
              int neighbor_index,
    int k_prime_index, int k_prime);
node * delete_entry( node * root, node * n, int key, void * pointer );
node * db_delete( node * root, int key );

void destroy_tree_nodes(node * root);
node * destroy_tree(node * root);

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
}