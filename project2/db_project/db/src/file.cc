#include "file.h"

namespace DSM{
    
    //maintain realpath and file descriptor list opened by open api call
    //(table_id, fd, realpath)
    table_info DB_FILE_LIST[MAX_DB_FILE_NUMBER];
    size_t DB_FILE_LIST_SIZE = 0;
    
    bool is_file_opened(int fd){
        //check fd in the DB_FILE_LIST
        for(int i=0;i<DB_FILE_LIST_SIZE;i++){
            if(DSM::DB_FILE_LIST[i].fd == fd) return true;
        }
        return false;
    }
    
    bool is_path_opened(const char* path){
        if(!path) return false; //NULL case
        //check path in the DB_FILE_LIST
        for(int i=0;i<DB_FILE_LIST_SIZE;i++){
            if(DSM::DB_FILE_LIST[i].fd>0 && 
                DSM::DB_FILE_LIST[i].path && 
                !strcmp(DSM::DB_FILE_LIST[i].path, path)) return true;
        }
        return false;
    }

    bool is_pagenum_valid(int fd, pagenum_t pagenum){
        //check fd is valid first
        if(!is_file_opened(fd)) return false;
        if(!pagenum) return true; //header page case

        //load header page data to get number of page attrib
        _dsm_page_t header_page;
        load_page_from_file(fd,0,&header_page._raw_page);

        //check boundary
        return pagenum < header_page._header_page.number_of_pages;
    }

    
    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  uint64_t number_of_pages){
        memset(pg, 0, sizeof(page_t)); //clear all field
        //use _dsm_page_t to reinterpret bitfield
        _dsm_page_t *dsm_pg = reinterpret_cast<_dsm_page_t*>(pg);
        //init header page with arg
        dsm_pg->_header_page.free_page_number = nxt_page_number;
        dsm_pg->_header_page.number_of_pages = number_of_pages;
    }

    void init_free_page(page_t* pg, pagenum_t nxt_page_number){
        memset(pg, 0, sizeof(page_t)); //clear all field
        //use _dsm_page_t to reinterpret bitfield
        _dsm_page_t *dsm_pg = reinterpret_cast<_dsm_page_t*>(pg);
        //init free page with arg
        dsm_pg->_free_page.nxt_free_page_number = nxt_page_number;
    }

    void store_page_to_file(int fd, pagenum_t pagenum, const page_t* src){
        //write page and sync
        //offset is pagenum * PAGE_SIZE
        if(pwrite64(fd,src,sizeof(page_t),pagenum*PAGE_SIZE)!=sizeof(page_t)){
            throw "write system call failed!";
        }
        if(fsync(fd)==-1) throw "sync system call failed!";
    }

    void load_page_from_file(int fd, pagenum_t pagenum, page_t* dest){
        //read page
        //offset is pagenum * PAGE_SIZE
        if(pread64(fd,dest,sizeof(page_t),pagenum*PAGE_SIZE)!=sizeof(page_t)){
            throw "read system call failed!";
        }
    }

    int get_file_descriptor(int64_t table_id){
        //scan in the DB_FILE_LIST
        for(int i=0;i<DB_FILE_LIST_SIZE;i++){
            if(DSM::DB_FILE_LIST[i].table_id == table_id) return DSM::DB_FILE_LIST[i].fd;
        }
        return -1;
    }
}


int64_t file_open_table_file(const char* pathname){
    //get realpath from given path
    //need memory-free before dump it
    //NULL return value means no existed file
    char* rpath = realpath(pathname, NULL);

    //check this path is already opened by this function
    if(DSM::is_path_opened(rpath)){
        free(rpath);
        throw "this file has been already opened";
    }

    //file descriptor
    int fd;

    //open file with RW mode
    if((fd=open64(pathname, O_RDWR)) == -1){
        //case when there is no such file
        //need to create and init db file

        //create file and open file with RW mode and check it's worked properly
        //permission is 644
        if((fd=open64(pathname, O_RDWR | O_CREAT, 0644)) == -1){
            throw "file_open_database_file failed";
        }

        page_t header_page, free_page;

        //init free page lists
        //init second page to last page to point next free page
        for(int i=1;i<DEFAULT_PAGE_NUMBER;i++){
            //init free page and save changes
            //use 0 to indicate end of list in last page
            //use i + 1 to point next free page
            //this make linked free page list sequentially
            DSM::init_free_page(&free_page, i+1>=DEFAULT_PAGE_NUMBER?0:i+1);
            DSM::store_page_to_file(fd, i, &free_page);
        }

        //init new db file's header page to point second page(first free page)
        DSM::init_header_page(&header_page, 1, DEFAULT_PAGE_NUMBER);
        //save changes in file
        DSM::store_page_to_file(fd, 0, &header_page);
    }

    //if create new db file now, make realpath again
    //it should change NULL to realpath string
    if(!rpath) rpath = realpath(pathname, NULL);
    
    //check list overflow
    if(DSM::DB_FILE_LIST_SIZE >= MAX_DB_FILE_NUMBER)
        throw "DB FILE LIST IS FULL";
    
    //insert file descriptor and realpath into list
    //to use for check duplicated open and close
    DSM::DB_FILE_LIST[DSM::DB_FILE_LIST_SIZE++] = {(int64_t)fd, rpath, fd};
    
    return (int64_t)fd; //set table_id as fd just for now
}

pagenum_t file_alloc_page(int64_t table_id){
    int fd; //file descriptor
    if((fd = DSM::get_file_descriptor(table_id)) == -1){
        throw "unvalid table id";
    }

    DSM::_dsm_page_t header_page;

    //read header page
    DSM::load_page_from_file(fd,0,&header_page._raw_page);

    //page number to alloc(return value)
    uint64_t nxt_page_number = header_page._header_page.free_page_number;
    
    if(nxt_page_number){
        //exist free page in list (no db file grow)

        //read free page to return
        DSM::_dsm_page_t nxt_page;
        DSM::load_page_from_file(fd,nxt_page_number,&nxt_page._raw_page);

        //make header page to point next free page in list
        header_page._header_page.free_page_number = nxt_page._free_page.nxt_free_page_number;
        //init new page
        DSM::init_free_page(&nxt_page._raw_page,0);

        //write changes in header page and allocated page
        DSM::store_page_to_file(fd,0,&header_page._raw_page);
        DSM::store_page_to_file(fd,nxt_page_number,&nxt_page._raw_page);
    }
    else{
        //no free page in list (db file grow occur)

        //get current number of page
        uint64_t current_number_of_pages = header_page._header_page.number_of_pages;
        //make new pages as much as current number of page
        size_t num_of_new_pages = current_number_of_pages;
        
        //set to be allocated page to position of first new page 
        nxt_page_number = current_number_of_pages;

        //init header page to point second new page and grow page size twice
        header_page._header_page.free_page_number = current_number_of_pages + 1;
        header_page._header_page.number_of_pages <<= 1;

        page_t free_page;

        //init free page lists
        //wipe out the first page before return the page number
        //init second page to last page to point next free page
        for(uint64_t i=0;i<num_of_new_pages;i++){
            //init free page and save changes
            //use 0 when need for wipe or indicate end of list
            //use current_number_of_pages + i + 1 to point next free page
            //this make linked free page list sequentially
            DSM::init_free_page(&free_page,  (i==0||i+1>=num_of_new_pages?0:current_number_of_pages+i+1));
            DSM::store_page_to_file(fd,current_number_of_pages + i,&free_page);
        }

        //save header file changes in file after making new free page list
        DSM::store_page_to_file(fd, 0, &header_page._raw_page);
    }

    return nxt_page_number;
}

void file_free_page(int64_t table_id, pagenum_t pagenum){
    int fd; //file descriptor
    if((fd = DSM::get_file_descriptor(table_id)) == -1){
        throw "unvalid table id";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_free_page";
    }

    //check pagenum is header page
    if(!pagenum){
        throw "free header page";
    }

    DSM::_dsm_page_t header_page, new_page;

    //read header page
    DSM::load_page_from_file(fd,0,&header_page._raw_page);

    //put the given page in the beginning of the list (LIFO)
    //init new page to point header page's nxt free page value
    DSM::init_free_page(&new_page._raw_page,header_page._header_page.free_page_number);
    //make header page to point given page number
    header_page._header_page.free_page_number = pagenum;
    
    //write changes in header page and freed page
    DSM::store_page_to_file(fd,pagenum,&new_page._raw_page);
    DSM::store_page_to_file(fd,0,&header_page._raw_page);
}

void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest){
    int fd; //file descriptor
    if((fd = DSM::get_file_descriptor(table_id)) == -1){
        throw "unvalid table id";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_read_page";
    }

    //call inner function
    DSM::load_page_from_file(fd,pagenum,dest);
}

void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src){
    int fd; //file descriptor
    if((fd = DSM::get_file_descriptor(table_id)) == -1){
        throw "unvalid table id";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_write_page";
    }

    //call inner function
    DSM::store_page_to_file(fd,pagenum,src);
}

void file_close_table_file(){
    //close all opened file descriptor
    for(int i=0;i<DSM::DB_FILE_LIST_SIZE;i++){
        DSM::table_info &it = DSM::DB_FILE_LIST[i];
        if(close(it.fd)==-1){
            throw "close db file failed";
        }
        free((void*)it.path); //free all path string
        it = {0,0,0}; //clear element

    }
    //clear list
    DSM::DB_FILE_LIST_SIZE = 0;
}