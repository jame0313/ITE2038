#pragma once
#include "page.h"
#include "wildcard.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <regex>

#define DEFAULT_PAGE_NUMBER 2560 //10MiB INIT DB SIZE
#define MAX_DB_FILE_NUMBER 32 //max number of table

// Open existing table file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_table_file();

//inner struct and function used in DiskSpaceManager
namespace DSM{
    //table(file) info struct
    struct table_info{
        int64_t table_id;
        char *path; //path to database file
        int fd; //file descriptor
        
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
    union _dsm_page_t {
        page_t _raw_page;
        DSM::header_page_t _header_page;
        DSM::free_page_t _free_page;
    };

    //check given file descriptor is valid(is this fd opened and not closed by DSM before)
    bool is_file_opened(int fd);
    //check given path is opened(is this pathed opened and not closed by DSM before)
    bool is_path_opened(const char* path);
    //check given pagenum is valid in fd(boundary check)
    bool is_pagenum_valid(int fd, pagenum_t pagenum);

    //init given page to header page format
    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  uint64_t number_of_pages);
    //init given page to free page format
    void init_free_page(page_t* pg, pagenum_t nxt_page_number);
    
    //inner function to store page to file
    void store_page_to_file(int fd, pagenum_t pagenum, const page_t* src);
    //inner function to load page from file
    void load_page_from_file(int fd, pagenum_t pagenum, page_t* dest);

    //get file descriptor corresponding to given table id, if not existed return -1
    int get_file_descriptor(int64_t table_id);
}

