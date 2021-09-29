#pragma once
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <set>
#include <map>

#define PAGE_SIZE 4096
#define DEFAULT_PAGE_NUMBER 2560
#define PAGE_NUMBER_OFFSET 0
#define NUMBER_OF_PAGES_OFFSET 8

typedef uint64_t pagenum_t;
struct page_t {
    uint8_t raw_data[PAGE_SIZE];
};

namespace DSM{
    struct str_compare{
        bool operator()(const char *str1, const char *str2) const;
    };

    union _dsm_page_t {
        struct page_t _raw_page;
        struct header_page_t{
            pagenum_t free_page_number;
            uint64_t number_of_pages;
            uint8_t __reserved__[PAGE_SIZE - sizeof(pagenum_t) - sizeof(uint64_t)];
        } _header_page;
        struct free_page_t{
            pagenum_t nxt_free_page_number;
            uint8_t __reserved__[PAGE_SIZE - sizeof(pagenum_t)];
        } _free_page;
    };

    bool is_file_opened(int fd);
    bool is_pagenum_valid(int fd, pagenum_t pagenum);

    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  uint64_t number_of_pages);
    void init_free_page(page_t* pg, pagenum_t nxt_page_number);
    
    void store_page_to_file(int fd, pagenum_t pagenum, const page_t* src);
    void load_page_from_file(int fd, pagenum_t pagenum, page_t* dest);
}

// Open existing database file or create one if not existed.
int file_open_database_file(const char* path);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in memory page structure( dest
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page( src ) to the on disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_database_file();