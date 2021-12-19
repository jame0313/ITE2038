#include "log.h"

namespace LGM{

    pthread_mutex_t log_buffer_latch = PTHREAD_MUTEX_INITIALIZER;

    uint64_t flushed_lsn;
    uint64_t global_lsn;
    std::queue<LGM::_log_record_t> log_buffer;

    int log_fd;
    FILE *log_msg_fp;

}