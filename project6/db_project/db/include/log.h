#pragma once

#include "page.h"
#include <stdio.h>
#include <queue>

#define NORMAL_RECOVERY 0
#define REDO_CRASH 1
#define UNDO_CRASH 2

enum log_msg_type{
    ANALYSIS_START,
    ANALYSIS_FINISH,
    PASS_START,
    PASS_END,
    TRX_BEGIN,
    TRX_UPDATE,
    TRX_COMMIT,
    TRX_ROLLBACK,
    TRX_COMPENSATE,
    TRX_CONSIDER_REDO
};


namespace LGM{
    struct trx_log_record_header_t{
        uint32_t log_size;
        uint64_t lsn;
        uint64_t prev_lsn;
        int trx_id;
        uint32_t type;
    } __attribute__((packed));

    struct update_log_record_header_t{
        uint32_t log_size;
        uint64_t lsn;
        uint64_t prev_lsn;
        int trx_id;
        uint32_t type;
        int64_t table_id;
        pagenum_t page_id;
        uint16_t offset;
        uint16_t size;
    } __attribute__((packed));

    typedef LGM::update_log_record_header_t compensate_log_record_header_t;

    struct trx_log_record{
        LGM::trx_log_record_header_t header;
    };

    struct update_log_record_t{
        LGM::update_log_record_header_t header;
        char *old_image;
        char *new_image;
    };

    struct compensate_log_record_t{
        LGM::compensate_log_record_header_t header;
        char *old_image;
        char *new_image;
        uint64_t prev_undo_lsn;
    };

    union _log_record_t{
        LGM::trx_log_record trx_record;
        LGM::update_log_record_t update_record;
        LGM::compensate_log_record_t compensate_record;
    };
    

}