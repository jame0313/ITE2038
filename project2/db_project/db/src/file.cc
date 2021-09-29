#include "file.h"

namespace DSM{
    
    std::set<int> DB_FILE_SET;
    std::map<const char*,int,DSM::str_compare> DB_PATH_MAP;

    bool str_compare::operator()(const char* str1, const char* str2) const{
        return strcmp(str1,str2)<0;
    }
    

    bool is_file_opened(int fd){
        return DB_FILE_SET.find(fd)!=DB_FILE_SET.end();
    }
    

    bool is_pagenum_valid(int fd, pagenum_t pagenum){
        if(!pagenum) return true;
        _dsm_page_t header_page;
        load_page_from_file(fd,0,&header_page._raw_page);
        return pagenum < header_page._header_page.number_of_pages;
    }

    
    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  uint64_t number_of_pages){
        memset(pg, 0, sizeof(page_t));
        _dsm_page_t *dsm_pg = reinterpret_cast<_dsm_page_t*>(pg);
        dsm_pg->_header_page.free_page_number = nxt_page_number;
        dsm_pg->_header_page.number_of_pages = number_of_pages;
    }

    void init_free_page(page_t* pg, pagenum_t nxt_page_number){
        memset(pg, 0, sizeof(page_t));
        _dsm_page_t *dsm_pg = reinterpret_cast<_dsm_page_t*>(pg);
        dsm_pg->_free_page.nxt_free_page_number = nxt_page_number;
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
    
    char* rpath = realpath(path, NULL);

    if(DSM::DB_PATH_MAP.find(rpath)!=DSM::DB_PATH_MAP.end()){
        throw "this file has been already opened";
    }

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

    if(!rpath) rpath = realpath(path, NULL);
    DSM::DB_PATH_MAP[rpath] = fd;
    
    return fd;
}

pagenum_t file_alloc_page(int fd){

    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    
    DSM::_dsm_page_t header_page;
    DSM::load_page_from_file(fd,0,&header_page._raw_page);

    uint64_t nxt_page_number = header_page._header_page.free_page_number;
    
    if(nxt_page_number){
        DSM::_dsm_page_t nxt_page;
        DSM::load_page_from_file(fd,nxt_page_number,&nxt_page._raw_page);

        header_page._header_page.free_page_number = nxt_page._free_page.nxt_free_page_number;
        DSM::init_free_page(&nxt_page._raw_page,0);

        DSM::store_page_to_file(fd,0,&header_page._raw_page);
        DSM::store_page_to_file(fd,nxt_page_number,&nxt_page._raw_page);
    }
    else{
        uint64_t current_number_of_pages = header_page._header_page.number_of_pages;
        size_t num_of_new_pages = current_number_of_pages;

        nxt_page_number = current_number_of_pages;

        DSM::init_header_page(&header_page._raw_page, current_number_of_pages+1, current_number_of_pages<<1);
        DSM::store_page_to_file(fd, 0, &header_page._raw_page);

        page_t free_page;

        for(uint64_t i=0;i<num_of_new_pages;i++){
            DSM::init_free_page(&free_page,  (i==0||i+1>=num_of_new_pages?0:current_number_of_pages+i+1));
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

    DSM::_dsm_page_t header_page, new_page;

    DSM::load_page_from_file(fd,0,&header_page._raw_page);
    DSM::load_page_from_file(fd,pagenum,&new_page._raw_page);

    DSM::init_free_page(&new_page._raw_page,header_page._header_page.free_page_number);
    header_page._header_page.free_page_number = pagenum;
    
    DSM::store_page_to_file(fd,0,&header_page._raw_page);
    DSM::store_page_to_file(fd,pagenum,&new_page._raw_page);
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

    for(auto &it : DSM::DB_PATH_MAP) free((void*)it.first);
    DSM::DB_PATH_MAP.clear();
}