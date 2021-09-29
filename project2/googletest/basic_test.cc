#include <gtest/gtest.h>
#include "file.h"
#include <vector>
#include <algorithm>

TEST(DiskSpaceManager, FileInitialization){
    const char* path = "./FileInitialization.db";
    int fd = file_open_database_file(path);
    
    loff_t siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10);

    DSM::_dsm_page_t header_page;
    file_read_page(fd,0,&header_page._raw_page);
    EXPECT_EQ(header_page._header_page.number_of_pages, DEFAULT_PAGE_NUMBER);

    file_close_database_file();
    remove(path);
}

TEST(DiskSpaceManager, PageManagement){
    const char* path = "./PageManagement.db";
    int fd = file_open_database_file(path);
    
    pagenum_t page1, page2;
    page1 = file_alloc_page(fd);
    page2 = file_alloc_page(fd);
    EXPECT_GT(page1,0);
    EXPECT_GT(page2,0);

    file_free_page(fd,page2);

    pagenum_t pn = 0;
    DSM::_dsm_page_t tmp;
    std::vector<pagenum_t> list;
    bool hasfreedpage = false;

    do{
        list.push_back(pn);
        file_read_page(fd,pn,&tmp._raw_page);
        
        EXPECT_NE(page1, pn);
        hasfreedpage |= pn == page2;

        pagenum_t np = pn?tmp._free_page.nxt_free_page_number:tmp._header_page.free_page_number;
        pn = np;
    }while(pn);
    
    EXPECT_TRUE(hasfreedpage);

    DSM::_dsm_page_t header_page;
    file_read_page(fd,0,&header_page._raw_page);
    EXPECT_EQ(list.size(),header_page._header_page.number_of_pages - 1);

    list.clear();
    file_close_database_file();
    remove(path);
}

TEST(DiskSpaceManager, PageIO){
    const char* path = "./PageIO.db";
    int fd = file_open_database_file(path);
    
    pagenum_t pg;
    pg = file_alloc_page(fd);
    EXPECT_GT(pg,0);

    page_t sample, target;

    char * str = new char[PAGE_SIZE];
    memset(str,'X',PAGE_SIZE);
    memcpy(&sample,str,PAGE_SIZE);

    file_write_page(fd,pg,&sample);
    file_read_page(fd,pg,&target);

    EXPECT_EQ(memcmp(&sample,&target,PAGE_SIZE),0);

    file_close_database_file();

    fd = file_open_database_file(path);

    file_read_page(fd,pg,&target);
    EXPECT_EQ(memcmp(&sample,&target,PAGE_SIZE),0);

    file_free_page(fd,pg);

    file_close_database_file();
    delete[] str;
    remove(path);
}

TEST(DiskSpaceManager, ErrorHandling){
    const char* path = "./ErrorHandling.db";
    const char* dup_path = "./././ErrorHandling.db";
    int fd = file_open_database_file(path);
    EXPECT_THROW(file_open_database_file(dup_path),const char*)<<"allowed duplicated open";
    
    page_t tmp={0,};
    EXPECT_THROW(file_read_page(fd+1,0,&tmp),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_write_page(fd+1,0,&tmp),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_alloc_page(fd+1),const char*)<<"allowed unvalid file descriptor";
    EXPECT_THROW(file_free_page(fd+1,0),const char*)<<"allowed unvalid file descriptor";

    DSM::_dsm_page_t header_page;
    file_read_page(fd,0,&header_page._raw_page);
    uint64_t num_of_page = header_page._header_page.number_of_pages;
    
    EXPECT_THROW(file_read_page(fd,num_of_page,&tmp),const char*)<<"allowed out of bound";
    EXPECT_THROW(file_write_page(fd,num_of_page,&tmp),const char*)<<"allowed out of bound";
    EXPECT_THROW(file_free_page(fd,num_of_page),const char*)<<"allowed out of bound";

    file_close_database_file();
    remove(path);
}

TEST(DiskSpaceManager, FileGrowTest){
    const char* path = "./FileGrowTest.db";
    int fd = file_open_database_file(path);
    
    std::vector<pagenum_t> pg_list;
    for(int i=0;i<DEFAULT_PAGE_NUMBER-1;i++){
        pg_list.push_back(file_alloc_page(fd));
        EXPECT_EQ(pg_list.back(),i+1);
    }
    
    loff_t siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10);

    DSM::_dsm_page_t header_page;
    file_read_page(fd,0,&header_page._raw_page);
    uint64_t num_of_page = header_page._header_page.number_of_pages;
    EXPECT_EQ(num_of_page,DEFAULT_PAGE_NUMBER);

    pg_list.push_back(file_alloc_page(fd));
    EXPECT_EQ(pg_list.back(),DEFAULT_PAGE_NUMBER);

    siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10*2);

    file_read_page(fd,0,&header_page._raw_page);
    
    pagenum_t nxt_free_page = header_page._header_page.free_page_number;
    num_of_page = header_page._header_page.number_of_pages;
    EXPECT_EQ(nxt_free_page,DEFAULT_PAGE_NUMBER+1);
    EXPECT_EQ(num_of_page,DEFAULT_PAGE_NUMBER*2);

    for(pagenum_t& x : pg_list){
        file_free_page(fd,x);
        file_read_page(fd,0,&header_page._raw_page);
        nxt_free_page = header_page._header_page.free_page_number;
        EXPECT_EQ(nxt_free_page, x);
    }

    DSM::_dsm_page_t tmp;
    std::vector<pagenum_t> free_pg_list;

    do{
        free_pg_list.push_back(nxt_free_page);
        file_read_page(fd,nxt_free_page,&tmp._raw_page);
        pagenum_t np = nxt_free_page?tmp._free_page.nxt_free_page_number:tmp._header_page.free_page_number;
        nxt_free_page = np;
    }while(nxt_free_page);
    EXPECT_EQ(free_pg_list.size(),DEFAULT_PAGE_NUMBER*2 - 1);

    free_pg_list.resize(pg_list.size());
    std::reverse(free_pg_list.begin(),free_pg_list.end());

    for(int i=0;i<pg_list.size();i++){
        EXPECT_EQ(pg_list[i],free_pg_list[i]);
    }
    


    free_pg_list.clear();
    pg_list.clear();
    file_close_database_file();
    remove(path);
}