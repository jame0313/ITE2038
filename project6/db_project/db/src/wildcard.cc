#include "wildcard.h"

void get_header_page_from_multiple_layer(int64_t table_id, page_t* dest){
    return buffer_read_page(table_id, 0, dest);
}

void set_header_page_from_multiple_layer(int64_t table_id, const page_t* src){
    buffer_write_page(table_id, 0, src);
    file_write_page(table_id, 0, src);
    return;
}