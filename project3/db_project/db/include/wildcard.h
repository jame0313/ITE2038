#pragma once
#include "page.h"
#include "bpt.h"
#include "buffer.h"
#include "file.h"

page_t get_header_page_from_multiple_layer(int64_t table_id);
