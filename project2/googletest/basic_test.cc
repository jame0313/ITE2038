#include <gtest/gtest.h>
#include "file.h"
#include <vector>

TEST(DiskSpaceManager, FileInitialization){
    const char* path = "./FileInitialization.db";
    int fd = file_open_database_file(path);
    
    loff_t siz = lseek64(fd,0,SEEK_END);
    EXPECT_EQ(siz, 1024*1024*10);

    page_t header_page;
    file_read_page(fd,0,&header_page);
    uint64_t num_of_page = DSM::read_data_from_page(&header_page,NUMBER_OF_PAGES_OFFSET);
    EXPECT_EQ(num_of_page,DEFAULT_PAGE_NUMBER);

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
    page_t tmp;
    std::vector<pagenum_t> list;
    bool hasfreedpage = false;
    do{
        list.push_back(pn);
        file_read_page(fd,pn,&tmp);
        pagenum_t np = DSM::read_data_from_page(&tmp,PAGE_NUMBER_OFFSET);
        EXPECT_NE(page1, pn);
        hasfreedpage |= pn == page2;
        pn = np;
    }while(pn);
    EXPECT_TRUE(hasfreedpage);

    page_t header_page;
    file_read_page(fd,0,&header_page);
    uint64_t num_of_page = DSM::read_data_from_page(&header_page,NUMBER_OF_PAGES_OFFSET);
    EXPECT_EQ(num_of_page,list.size()+1);

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

    char * str = new char[4096];
    for(int i=0;i<4096;i++) str[i] = 'X';
    memcpy(&sample,str,4096);

    file_write_page(fd,pg,&sample);
    file_read_page(fd,pg,&target);

    EXPECT_EQ(memcmp(&sample,&target,4096),0);

    file_free_page(fd,pg);

    file_close_database_file();
    remove(path);
}