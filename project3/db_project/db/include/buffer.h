#pragma once

#include "page.h"
#include "file.h"
#include <unordered_map>

typedef page_t frame_t;
typedef uint64_t framenum_t;

//allocate the buffer pool with the given number of entries
int init_buffer(int num_buf);

// Open existing table file or create one if it doesn't exist
int64_t buffer_open_table_file(const char* pathname);

// Allocate a page
pagenum_t buffer_alloc_page(int64_t table_id);

// Free a page
void buffer_free_page(int64_t table_id, pagenum_t pagenum);

// get a page pointer from buffer
page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum);

// Write a page to buffer
void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);

// Flush all and destroy
void buffer_close_table_file();

namespace BM{
    frame_t *frame_list;
    BM::ctrl_blk *ctrl_blk_list;

    std::unordered_map<pagenum_t, ctrl_blk*> hash_table;

    struct ctrl_blk{
        frame_t* frame;
        int64_t table_id;
        pagenum_t pagenum;
        framenum_t prv_lru_ptr;
        framenum_t nxt_lru_ptr;
        bool is_dirty;
        uint32_t is_pinned;
    };
}