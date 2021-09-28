#include "file.h"

namespace DSM{
    
    std::set<int> DB_FILE_SET;

    bool is_file_opened(int fd){
        return DB_FILE_SET.find(fd)!=DB_FILE_SET.end();
    }

    bool is_pagenum_valid(int fd, pagenum_t pagenum){
        if(!pagenum) return true;
        page_t header_page;
        load_page_from_file(fd,0,&header_page);
        uint64_t number_of_pages = read_data_from_page(&header_page,NUMBER_OF_PAGES_OFFSET);
        return pagenum < number_of_pages;
    }

    
    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  uint64_t number_of_pages){
        memset(pg, 0, sizeof(page_t));
        *reinterpret_cast<pagenum_t*>(pg->raw_data + PAGE_NUMBER_OFFSET) = nxt_page_number;
        *reinterpret_cast<uint64_t*>(pg->raw_data + NUMBER_OF_PAGES_OFFSET) = number_of_pages;
    }

    void init_free_page(page_t* pg, pagenum_t nxt_page_number){
        memset(pg, 0, sizeof(page_t));
        *reinterpret_cast<pagenum_t*>(pg->raw_data + PAGE_NUMBER_OFFSET) = nxt_page_number;
    }

    uint64_t read_data_from_page(const page_t* pg, uint64_t offset){
        return *reinterpret_cast<const uint64_t*>(pg->raw_data + offset);
    }

    void write_data_from_page(page_t* pg, uint64_t offset, uint64_t val){
        *reinterpret_cast<uint64_t*>(pg->raw_data + offset) = val;
    }

    void store_page_to_file(int fd, pagenum_t pagenum, const page_t* src){
        pwrite64(fd,src,sizeof(page_t),pagenum*PAGE_SIZE);
        fsync(fd);
    }

    void load_page_from_file(int fd, pagenum_t pagenum, page_t* dest){
        pread64(fd,dest,sizeof(page_t),pagenum*PAGE_SIZE);
    }
}


int file_open_database_file(const char* path){
    
    int fd;

    if((fd=open64(path, O_RDWR)) == -1){
        if((fd=open64(path, O_RDWR | O_CREAT, 0644)) == -1){
            throw "file_open_database_file failed";
        }

        page_t header_page, free_page;

        DSM::init_header_page(&header_page, 1, DEFAULT_PAGE_NUMBER);
        DSM::store_page_to_file(fd, 0, &header_page);

        for(int i=1;i<DEFAULT_PAGE_NUMBER;i++){
            DSM::init_free_page(&free_page, i+1>=DEFAULT_PAGE_NUMBER?0:i+1);
            DSM::store_page_to_file(fd, i, &free_page);
        }
    }

    DSM::DB_FILE_SET.insert(fd);
    
    return fd;
}

pagenum_t file_alloc_page(int fd){
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    
    page_t header_page;

    DSM::load_page_from_file(fd,0,&header_page);

    uint64_t nxt_page_number = DSM::read_data_from_page(&header_page,PAGE_NUMBER_OFFSET);
    
    if(nxt_page_number){
        page_t nxt_page;
        DSM::load_page_from_file(fd,nxt_page_number,&nxt_page);

        uint64_t nxt_nxt_page_number = DSM::read_data_from_page(&nxt_page,PAGE_NUMBER_OFFSET);
        DSM::write_data_from_page(&header_page,PAGE_NUMBER_OFFSET,nxt_nxt_page_number);
        DSM::init_free_page(&nxt_page,0);

        DSM::store_page_to_file(fd,0,&header_page);
        DSM::store_page_to_file(fd,nxt_page_number,&nxt_page);
    }
    else{
        uint64_t current_number_of_pages = DSM::read_data_from_page(&header_page,NUMBER_OF_PAGES_OFFSET);
        size_t num_of_new_pages = current_number_of_pages;

        nxt_page_number = current_number_of_pages;

        DSM::init_header_page(&header_page, current_number_of_pages+1, current_number_of_pages<<1);
        DSM::store_page_to_file(fd,0,&header_page);

        page_t free_page;

        for(uint64_t i=0;i<num_of_new_pages;i++){
            DSM::init_free_page(&free_page, current_number_of_pages + (i==0||i+1>=current_number_of_pages?0:i+1));
            DSM::store_page_to_file(fd,current_number_of_pages + i,&free_page);
        }

    }

    return nxt_page_number;
}

void file_free_page(int fd, pagenum_t pagenum){
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in fie_read_page";
    }

    page_t header_page, new_page;

    DSM::load_page_from_file(fd,0,&header_page);
    DSM::load_page_from_file(fd,pagenum,&new_page);

    uint64_t header_page_nxt_page = DSM::read_data_from_page(&header_page,PAGE_NUMBER_OFFSET);
    DSM::init_free_page(&new_page,header_page_nxt_page);
    DSM::write_data_from_page(&header_page,PAGE_NUMBER_OFFSET,pagenum);
    
    
    DSM::store_page_to_file(fd,0,&header_page);
    DSM::store_page_to_file(fd,pagenum,&new_page);
}

void file_read_page(int fd, pagenum_t pagenum, page_t* dest){
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_read_page";
    }
    DSM::load_page_from_file(fd,pagenum,dest);
}

void file_write_page(int fd, pagenum_t pagenum, const page_t* src){
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_write_page";
    }

    DSM::store_page_to_file(fd,pagenum,src);
    
}

void file_close_database_file(){
    
    for(int fd : DSM::DB_FILE_SET) close(fd);

    DSM::DB_FILE_SET.clear();
}