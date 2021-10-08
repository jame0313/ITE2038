#pragma once
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <utility>

#define PAGE_SIZE 4096 //4KiB page
#define DEFAULT_PAGE_NUMBER 2560 //10MiB INIT DB SIZE
#define MAX_DB_FILE_NUMBER 32 //max number of table

typedef uint64_t pagenum_t; //page_number
struct page_t {
    //in-memory page structure
    uint8_t raw_data[PAGE_SIZE];
};

// Open existing database file or create one if it doesn't exist
int file_open_database_file(const char* path);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_database_file();

//inner struct and function used in DiskSpaceManager
namespace DSM{
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
}

