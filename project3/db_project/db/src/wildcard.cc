#include "wildcard.h"

page_t get_header_page_from_multiple_layer(int64_t table_id){
    page_t ret_page;
    memset(&ret_page, 0, sizeof(page_t));
    buffer_read_page(table_id, 0, &ret_page);
    return ret_page;
}

void set_header_page_from_multiple_layer(int64_t table_id, const page_t* src){
    buffer_write_page(table_id, 0, src);
}