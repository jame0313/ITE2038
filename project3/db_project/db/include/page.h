#pragma once
#include <stdint.h>

#define PAGE_SIZE 4096 //4KiB page

typedef uint64_t pagenum_t; //page_number
struct page_t {
    //in-memory page structure
    uint8_t raw_data[PAGE_SIZE];
};