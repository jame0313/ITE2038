#include <stdint.h>
#include <stdio.h>
#include <string.h>
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
    int64_t DB_FILE_SIZE = 0;
    std::map<int64_t,FILE*> DB_FILE_MAP;

    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  int64_t number_of_pages);
    void init_free_page(page_t* pg, pagenum_t nxt_page_number);
}
int64_t file_open_database_file(char* path);

pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, page_t* dest);

void file_write_page(pagenum_t pagenum, const page_t* src);

void file_close_database_file();