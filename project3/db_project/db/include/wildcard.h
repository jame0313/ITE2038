#pragma once
#include "page.h"
#include "bpt.h"
#include "buffer.h"
#include "file.h"

//get header page from multiple layer
//try to get header page from upper layer to lower layer
//copy header page to dest
void get_header_page_from_multiple_layer(int64_t table_id, page_t* dest);

//set header page from multiple layer
//try to set header page from upper layer to lower layer
//copy src to header page
void set_header_page_from_multiple_layer(int64_t table_id, const page_t* src);