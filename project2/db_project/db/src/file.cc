#include "file.h"

namespace DSM{
    
    std::set<int> DB_FILE_SET; //maintain file descriptor list opened by open api call
    std::map<char*,int,DSM::str_compare> DB_PATH_MAP; //maintain (realpath,file descriptor) list opened by open api call

    bool str_compare::operator()(const char* str1, const char* str2) const{
        //alphabetical order
        return strcmp(str1,str2)<0;
    }
    

    bool is_file_opened(int fd){
        //check fd in the set of DB_FILE_SET
        return DB_FILE_SET.find(fd)!=DB_FILE_SET.end();
    }
    

    bool is_pagenum_valid(int fd, pagenum_t pagenum){
        //check fd is valid first
        if(DB_FILE_SET.find(fd)==DB_FILE_SET.end()) return false;
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
        pwrite64(fd,src,sizeof(page_t),pagenum*PAGE_SIZE);
        fsync(fd);
    }

    void load_page_from_file(int fd, pagenum_t pagenum, page_t* dest){
        //read page
        //offset is pagenum * PAGE_SIZE
        pread64(fd,dest,sizeof(page_t),pagenum*PAGE_SIZE);
    }
}


int file_open_database_file(const char* path){
    //get realpath from given path
    //need memory-free before dump it
    //NULL return value means no existed file
    char* rpath = realpath(path, NULL);

    //check this path is already opened by this function
    if(DSM::DB_PATH_MAP.find(rpath)!=DSM::DB_PATH_MAP.end()){
        throw "this file has been already opened";
    }

    //file descriptor
    int fd;

    //open file with RW mode
    if((fd=open64(path, O_RDWR)) == -1){
        //case when there is no such file
        //need to create and init db file

        //create file and open file with RW mode and check it's worked properly
        //permission is 644
        if((fd=open64(path, O_RDWR | O_CREAT, 0644)) == -1){
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

    //insert file descriptor into set to use for close
    DSM::DB_FILE_SET.insert(fd);

    //if create new db file now, make realpath again
    //it should change NULL to realpath string
    if(!rpath) rpath = realpath(path, NULL);
    
    //insert realpath into map to use for check duplicated open
    DSM::DB_PATH_MAP[rpath] = fd;
    
    return fd;
}

pagenum_t file_alloc_page(int fd){
    //check fd is valid
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
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
        DSM::init_header_page(&header_page._raw_page, current_number_of_pages+1, current_number_of_pages<<1);
        
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

void file_free_page(int fd, pagenum_t pagenum){
    //check fd is valid
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_free_page";
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

void file_read_page(int fd, pagenum_t pagenum, page_t* dest){
    //check fd is valid
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_read_page";
    }

    //call inner function
    DSM::load_page_from_file(fd,pagenum,dest);
}

void file_write_page(int fd, pagenum_t pagenum, const page_t* src){
    //check fd is valid
    if(!DSM::is_file_opened(fd)){
        throw "unvalid file descriptor";
    }
    //check pagenum is valid
    if(!DSM::is_pagenum_valid(fd,pagenum)){
        throw "pagenum is out of bound in file_write_page";
    }

    //call inner function
    DSM::store_page_to_file(fd,pagenum,src);
}

void file_close_database_file(){
    //close all opened file descriptor
    for(int fd : DSM::DB_FILE_SET) close(fd);
    //free all path string    
    for(auto &it : DSM::DB_PATH_MAP) free((void*)it.first);

    //clear set and map
    DSM::DB_FILE_SET.clear();
    DSM::DB_PATH_MAP.clear();
}