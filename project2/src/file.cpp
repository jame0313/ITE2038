#include "file.h"

namespace DSM{

    void init_header_page(page_t* pg, pagenum_t nxt_page_number,  int64_t number_of_pages){
        memset(pg, 0, sizeof page_t);
        *reinterpret_cast<pagenum_t*>(pg->raw_data + PAGE_NUMBER_OFFSET) = nxt_page_number;
        *reinterpret_cast<int64_t*>(pg->raw_data + NUMBER_OF_PAGES_OFFSET) = number_of_pages;
    }

    void init_free_page(page_t* pg, pagenum_t nxt_page_number){
        memset(pg, 0, sizeof page_t);
        *reinterpret_cast<pagenum_t*>(pg->raw_data + PAGE_NUMBER_OFFSET) = nxt_page_number;
    }
}


int64_t file_open_database_file(char* path){
    FILE* fp = fopen(path,"rb+");

    if(!fp){
        fp = fopen(path,"wb+");
        page_t header_page, free_page;

        DSM::init_header_page(&header_page, 1, DEFAULT_PAGE_NUMBER);
        fwrite(&header_page, sizeof page_t, 1, fp);

        for(int i=1;i<DEFAULT_PAGE_NUMBER;i++){
            DSM::init_free_page(&free_page, i+1>=DEFAULT_PAGE_NUMBER?0:i+1);
            fwrite(&free_page, sizeof page_t, 1, fp);
        }

        fflush(fp);
    }

    int64_t return_id = DSM::DB_FILE_SIZE;
    DSM::DB_FILE_MAP[DSM::DB_FILE_SIZE++] = fp;
    
    return return_id;
}

pagenum_t file_alloc_page(){
    if(DSM::DB_FILE_SIZE<=0){
        throw "you should call file_open_database_file first!";
    }
    
    page_t header_page;
    file_read_page(0, &header_page);
    pagenum_t &nxt_page_number = *reinterpret_cast<pagenum_t*>(header_page.raw_data + PAGE_NUMBER_OFFSET);
    
    if(nxt_page_number){
        page_t nxt_page;
        file_read_page(nxt_page_number, &nxt_page);
        nxt_page_number = *reinterpret_cast<pagenum_t*>(nxt_page.raw_data + PAGE_NUMBER_OFFSET);
        file_write_page(0, &header_page);
        return nxt_page_number;
    }
    else{
        int64_t &current_number_of_pages = *reinterpret_cast<int64_t*>(header_page.raw_data + NUMBER_OF_PAGES_OFFSET);
        size_t num_of_new_pages = current_number_of_pages;

        nxt_page_number = current_number_of_pages;
        current_number_of_pages += num_of_new_pages;

        file_write_page(0, &header_page);

        FILE* fp = DSM::DB_FILE_MAP.rbegin()->second;
        fseek(fp, 0, SEEK_END);

        page_t free_page;

        for(int i=0;i<num_of_new_pages;i++){
            DSM::init_free_page(&free_page, current_number_of_pages + (i+1>=DEFAULT_PAGE_NUMBER?0:i+1));
            fwrite(&free_page, sizeof page_t, 1, fp);
        }

        fflush(fp);
    }
}

void file_free_page(pagenum_t pagenum){
    if(DSM::DB_FILE_SIZE<=0){
        throw "you should call file_open_database_file first!";
    }

    page_t header_page, new_page;

    file_read_page(0, &header_page);
    file_read_page(pagenum, &new_page);
    
    *reinterpret_cast<pagenum_t*>(new_page.raw_data + PAGE_NUMBER_OFFSET) = *reinterpret_cast<pagenum_t*>(header_page.raw_data + PAGE_NUMBER_OFFSET);
    *reinterpret_cast<pagenum_t*>(header_page.raw_data + PAGE_NUMBER_OFFSET) = pagenum;

    file_write_page(0, &header_page);
    file_write_page(pagenum, &new_page);
}

void file_read_page(pagenum_t pagenum, page_t* dest){
    if(DSM::DB_FILE_SIZE<=0){
        throw "you should call file_open_database_file first!";
    }

    FILE* fp = DSM::DB_FILE_MAP.rbegin()->second;
    fseek(fp, PAGE_SIZE*pagenum, SEEK_SET);

    fread(dest, sizeof page_t, 1, fp);
}

void file_write_page(pagenum_t pagenum, const page_t* src){
    if(DSM::DB_FILE_SIZE<=0){
        throw "you should call file_open_database_file first!";
    }

    FILE* fp = DSM::DB_FILE_MAP.rbegin()->second;
    fseek(fp, PAGE_SIZE*pagenum, SEEK_SET);

    fwrite(src, sizeof page_t, 1, fp);
    fflush(fp);
}

void file_close_database_file(){
    if(DSM::DB_FILE_SIZE<=0){
        throw "you should call file_open_database_file first!";
    }

    for(auto& it : DSM::DB_FILE_MAP){
        if(it.second) fclose(it.second);
    }
}