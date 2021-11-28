#pragma once

#include "page.h"
#include "file.h"
#include <unordered_map>
#include <utility>
#include <pthread.h>

#define DEFAULT_BUFFER_SIZE 1024

typedef page_t frame_t;
typedef int64_t framenum_t;
typedef int64_t blknum_t;
typedef std::pair<int64_t, pagenum_t> page_id;

//allocate the buffer pool with the given number of entries
//return 0 if success or non-zero if fail
int init_buffer(int num_buf = DEFAULT_BUFFER_SIZE);

// Allocate a page
pagenum_t buffer_alloc_page(int64_t table_id);

// Free a page
void buffer_free_page(int64_t table_id, pagenum_t pagenum);

// read a page from buffer
// addiditonal flag(mode) is for locking policy
// mode is 0(exclusive lock), 1(chk exclusive lock only, no locking), or 2(shared lock) 
void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest, int mode = 0);

// Write a page to buffer and release page latch
void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);

// Flush all and destroy
void buffer_close_table_file();

//inner struct and function used in BufferManager
namespace BM{

    //buffer control block structure
    struct ctrl_blk{
        frame_t* frame_ptr; //point to real frame(page)
        int64_t table_id;
        pagenum_t pagenum;
        blknum_t lru_prv_blk_number; //prev block number in LRU list or -1 if not existed
        blknum_t lru_nxt_blk_number; //next block number in LRU list or -1 if not existed
        bool is_dirty; //set on if it need flush (identify content's changes)
        pthread_rwlock_t page_latch = PTHREAD_RWLOCK_INITIALIZER; //identify this buffer is-use
        pthread_cond_t cond = PTHREAD_COND_INITIALIZER; //cond var for sleeping
    };

    //inner structure for hashing pair object
    //hash algorithm used in boost lib + std::hash
    // https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
    struct hash_pair {
        template <class T1, class T2>
        size_t operator()(const std::pair<T1, T2>& p) const;
    };

    //header page(first page) structure
    struct header_page_t{
        pagenum_t free_page_number; //point to the first free page(head of free page list) or indicate no free page if 0
        uint64_t number_of_pages; //the number of pages paginated in db file
        uint8_t __reserved__[PAGE_SIZE - sizeof(pagenum_t) - sizeof(uint64_t)]; //not used for now
    };

    //free page structure
    struct free_page_t{
        pagenum_t nxt_free_page_number; //point to the next free page or indicate end of the free page list if 0
        uint8_t __reserved__[PAGE_SIZE - sizeof(pagenum_t)]; //not used for now
    };

    //union all type of page to reinterpret shared bitfield by using each type's member variable
    union _bm_page_t {
        frame_t _raw_frame;
        BM::header_page_t _header_page;
        BM::free_page_t _free_page;
    };

    //flush frame in given control block 
    void flush_frame_to_file(blknum_t blknum);

    //find ctrl block number in hash table
    //where it's pagenum and table id is same with given parameter
    //return ctrl block number or -1 if not found
    blknum_t find_ctrl_blk_in_hash_table(int64_t table_id, pagenum_t pagenum);

    //find victim block for eviction by following the LRU policy
    //return ctrl block number or -1 if not found(i.e. all pinned)
    blknum_t find_victim_blk_from_buffer();

    //move given block to end of LRU list
    //by reconnecting some block's pointer
    //caused by page access
    void move_blk_to_end(blknum_t blknum);

    //get ctrl block from buffer (core function)
    //find block in buffer or get from disk
    //return control block pointer or
    //throw msg if it can't evict
    ctrl_blk* get_ctrl_blk_from_buffer(int64_t table_id, pagenum_t pagenum);
}