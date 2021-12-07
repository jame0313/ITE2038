#include <gtest/gtest.h>
#include "file.h"
#include "api.h"
#include <vector>
#include <algorithm>

// When a file is newly created, a file of 10MiB is created and The number of pages
// corresponding to 10MiB should be created. Check the "Number of pages" entry in the
// header page.
TEST(DiskSpaceManager, FileInitialization){
    //init test
    const char* path = "./FileInitialization.db";
    init_db();
    int64_t tid = file_open_table_file(path);
    int fd = DSM::get_file_descriptor(tid);


    //check file size is 10MiB
    loff_t siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10);

    //read header page and check the number of pages entry
    DSM::_dsm_page_t header_page;
    file_read_page(tid,0,&header_page._raw_page);
    EXPECT_EQ(header_page._header_page.number_of_pages, DEFAULT_PAGE_NUMBER);


    //end test
    EXPECT_NE(fcntl(fd,F_GETFL),-1); //check fd not closed yet
    file_close_table_file();
    EXPECT_EQ(fcntl(fd,F_GETFL),-1); //check fd closed properly
    remove(path);
}

// Allocate two pages by calling file_alloc_page() twice, and free one of them by calling
// file_free_page(). After that, iterate the free page list by yourself and check if only the freed
// page is inside the free list.
// (test for file_alloc_page and file_free_page)
TEST(DiskSpaceManager, PageManagement){
    //init test
    const char* path = "./PageManagement.db";
    init_db();
    int64_t tid = file_open_table_file(path);
    int fd = DSM::get_file_descriptor(tid);
    
    //allocate two page and check status
    pagenum_t page1, page2;
    page1 = file_alloc_page(tid);
    page2 = file_alloc_page(tid);
    EXPECT_GT(page1,0);
    EXPECT_GT(page2,0);
    EXPECT_NE(page1, page2);

    //free one of them
    file_free_page(tid,page2);

    pagenum_t pn = 0; //point to header page
    DSM::_dsm_page_t tmp; //free page list
    std::vector<pagenum_t> list;
    bool hasfreedpage = false; //checker for page2

    do{
        EXPECT_NE(page1, pn); //check page1 is not in free page list
        //get next free page number
        file_read_page(tid,pn,&tmp._raw_page);
        pagenum_t np = pn?tmp._free_page.nxt_free_page_number:tmp._header_page.free_page_number;

        if(!pn) hasfreedpage = np == page2; //this page is just freed page
        pn = np; //go next page
        if(pn) list.push_back(pn); //make free page list
    }while(pn);
    EXPECT_TRUE(hasfreedpage); //check page2 is in free page list at the beginning of list

    //check free page list size is same as (total_page_number - 1(header) - 1(allocated page))
    DSM::_dsm_page_t header_page;
    file_read_page(tid,0,&header_page._raw_page);
    EXPECT_EQ(list.size(),header_page._header_page.number_of_pages - 2);

    //end test
    list.clear();
    file_close_table_file();
    remove(path);
}

// After allocating a new page, write any 4096 bytes of value (e.g., " aaaa …") to the page by
// calling the file_write_page () function. After reading the page again by calling the
// file_read_page () function, check whether the read page and the written content are the
// same.
// (test for file_write_page and file_read_page)
TEST(DiskSpaceManager, PageIO){
    //init page
    const char* path = "./PageIO.db";
    init_db();
    int64_t tid = file_open_table_file(path);
    int fd = DSM::get_file_descriptor(tid);
    
    //allocate one page
    pagenum_t pg;
    pg = file_alloc_page(tid);
    EXPECT_GT(pg,0);

    //use for check matching
    page_t sample, target;

    //init sample filling 4096 byte with 'X'
    char * str = new char[PAGE_SIZE];
    memset(str,'X',PAGE_SIZE);
    memcpy(&sample,str,PAGE_SIZE);

    //write from sample and read to target
    file_write_page(tid,pg,&sample);
    file_read_page(tid,pg,&target);

    //check two page contents are identical
    EXPECT_EQ(memcmp(&sample,&target,PAGE_SIZE),0);

    //close db and open same db file again
    file_close_table_file();
    tid = file_open_table_file(path);
    fd = DSM::get_file_descriptor(tid);


    //check two page contents are identical again
    file_read_page(tid,pg,&target);
    EXPECT_EQ(memcmp(&sample,&target,PAGE_SIZE),0);

    //free page
    file_free_page(tid,pg);

    //end test
    file_close_table_file();
    delete[] str;
    remove(path);
}

// test for various error handling
// (duplicated open, unvalid fd, out of bound)
TEST(DiskSpaceManager, ErrorHandling){
    //init test
    const char* path = "./ErrorHandling.db";
    const char* dup_path = "./././ErrorHandling.db";
    init_db();
    int64_t tid = file_open_table_file(path);
    int fd = DSM::get_file_descriptor(tid);

    //check duplicated open
    EXPECT_THROW(file_open_table_file(dup_path),const char*)<<"allowed duplicated open";
    
    //check unvalid file descriptor
    page_t tmp={0,};
    EXPECT_THROW(file_read_page(tid+1,0,&tmp),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_write_page(tid+1,0,&tmp),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_alloc_page(tid+1),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_free_page(tid+1,0),const char*)<<"allowed unvalid file descriptor";

    //use for out of bound check
    DSM::_dsm_page_t header_page;
    file_read_page(tid,0,&header_page._raw_page);
    uint64_t num_of_page = header_page._header_page.number_of_pages;
    
    //check out of bound
    EXPECT_THROW(file_read_page(tid,num_of_page,&tmp),const char*)<<"allowed out of bound";
    EXPECT_THROW(file_write_page(tid,num_of_page,&tmp),const char*)<<"allowed out of bound";
    EXPECT_THROW(file_free_page(tid,num_of_page),const char*)<<"allowed out of bound";

    //check header page case
    EXPECT_THROW(file_free_page(tid,0),const char*)<<"allowed header page free";


    //end test
    file_close_table_file();
    remove(path);
}

// When new pages are allocated by calling file_alloc_page (), new pages must be
// allocated as much as the current DB size.
// check doubling the current file space when there is no free page
// and caller requests it
TEST(DiskSpaceManager, FileGrowTest){
    //init test
    const char* path = "./FileGrowTest.db";
    init_db();
    int64_t tid = file_open_table_file(path);
    int fd = DSM::get_file_descriptor(tid);
    
    //allocate all free page in the list
    std::vector<pagenum_t> pg_list;
    for(int i=0;i<DEFAULT_PAGE_NUMBER-1;i++){
        pg_list.push_back(file_alloc_page(tid));
        EXPECT_EQ(pg_list.back(),i+1); //check page status
    }
    
    //check the file size doesn't grow yet
    loff_t siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10);

    //check the number of page doesn't increase yet
    //and no free page left
    DSM::_dsm_page_t header_page;
    file_read_page(tid,0,&header_page._raw_page);
    pagenum_t nxt_free_page = header_page._header_page.free_page_number;
    uint64_t num_of_page = header_page._header_page.number_of_pages;
    EXPECT_EQ(num_of_page,DEFAULT_PAGE_NUMBER);
    EXPECT_EQ(nxt_free_page,0);

    //allocate one more to make file grow
    pg_list.push_back(file_alloc_page(tid));
    EXPECT_EQ(pg_list.back(),DEFAULT_PAGE_NUMBER);

    //check file size growth
    siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10*2);

    //check doubling the number of page and next free page status
    file_read_page(tid,0,&header_page._raw_page);
    nxt_free_page = header_page._header_page.free_page_number;
    num_of_page = header_page._header_page.number_of_pages;
    EXPECT_EQ(nxt_free_page,DEFAULT_PAGE_NUMBER+1);
    EXPECT_EQ(num_of_page,DEFAULT_PAGE_NUMBER*2);

    //free all allocated page
    for(pagenum_t& x : pg_list){
        file_free_page(tid,x);
    }

    DSM::_dsm_page_t tmp;
    std::vector<pagenum_t> free_pg_list;

    //read all free page list
    file_read_page(tid,0,&header_page._raw_page);
    nxt_free_page = header_page._header_page.free_page_number;
    do{
        free_pg_list.push_back(nxt_free_page);
        file_read_page(tid,nxt_free_page,&tmp._raw_page);
        pagenum_t np = nxt_free_page?tmp._free_page.nxt_free_page_number:tmp._header_page.free_page_number;
        nxt_free_page = np;
    }while(nxt_free_page);

    //check the number of free page also almost doubled (2*num(prev total page)-1(header page))
    EXPECT_EQ(free_pg_list.size(),DEFAULT_PAGE_NUMBER*2 - 1);

    //populate first half(first 10MiB part) to check LIFO
    //it's order is reverse of original one
    free_pg_list.resize(pg_list.size());
    std::reverse(free_pg_list.begin(),free_pg_list.end());

    for(int i=0;i<pg_list.size();i++){
        EXPECT_EQ(pg_list[i],free_pg_list[i]);
    }
    
    //end test
    free_pg_list.clear();
    pg_list.clear();
    file_close_table_file();
    remove(path);
}