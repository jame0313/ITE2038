#include "buffer.h"

namespace BM{
    frame_t *frame_list; //frame(page) array
    BM::ctrl_blk *ctrl_blk_list; //ctrl block(frame_ptr + info) array

    //hash table that mapping ctrl block in the list
    //search key is page_id({table_id, pagenum})
    //value is block num
    std::unordered_map<page_id, blknum_t, BM::hash_pair> hash_table;

    size_t BUFFER_SIZE = 0;

    //LRU list pointer
    //front point LRU block and back point MRU block
    blknum_t ctrl_blk_list_front;
    blknum_t ctrl_blk_list_back;

    //code by boost lib
    // https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
    template <class T1, class T2>
    size_t BM::hash_pair::operator()(const std::pair<T1, T2>& p) const {
        auto hash1 = std::hash<T1>{}(p.first);
        auto hash2 = std::hash<T2>{}(p.second);
        //use magic number and some bit shift to fit hash feature
        return hash1 ^ hash2 + 0x9e3779b9 + (hash2<<6) + (hash2>>2);
    }

    bool is_pagenum_valid(int64_t table_id, pagenum_t pagenum){
        if(!pagenum) return true; //header page case

        //load header page data to get number of page attrib
        BM::ctrl_blk *header_blk;
        BM::_bm_page_t *header_page;

        header_blk = BM::get_ctrl_blk_from_buffer(table_id, 0); //get header page block

        header_page = reinterpret_cast<BM::_bm_page_t*>(header_blk->frame_ptr);

        //check boundary
        return pagenum < header_page->_header_page.number_of_pages;
    }

    void init_free_page(frame_t* pg, pagenum_t nxt_page_number){
        memset(pg, 0, sizeof(page_t)); //clear all field
        //use _bm_page_t to reinterpret bitfield
        _bm_page_t *bm_pg = reinterpret_cast<_bm_page_t*>(pg);
        //init free page with arg
        bm_pg->_free_page.nxt_free_page_number = nxt_page_number;
    }

    void flush_frame_to_file(blknum_t blknum){
        //get given block in the list
        ctrl_blk* blk = &BM::ctrl_blk_list[blknum];
        //call write DSM api
        file_write_page(blk->table_id,blk->pagenum,blk->frame_ptr);
    }

    blknum_t find_ctrl_blk_in_hash_table(int64_t table_id, pagenum_t pagenum){
        page_id pid = {table_id, pagenum}; //make page_id to use as search key in hash table
        if(BM::hash_table.find(pid)!=BM::hash_table.end()){
            //found case
            //return corresponging block number
            return BM::hash_table[pid];
        }
        else{
            //not found case
            //return -1
            return -1;
        }
    }

    blknum_t find_victim_blk_from_buffer(){
        blknum_t cnt_blk = ctrl_blk_list_front; //get LRU block
        while(cnt_blk != ctrl_blk_list_back && BM::ctrl_blk_list[cnt_blk].is_pinned){
            //get nxt LRU block
            //since current block is pinned (can't evict)
            cnt_blk = BM::ctrl_blk_list[cnt_blk].lru_nxt_blk_number;
        }
        if(!BM::ctrl_blk_list[cnt_blk].is_pinned){
            //found case
            //return corresponging block number
            return cnt_blk;
        }
        else{
            //not found case
            //return -1
            return -1;
        }
    }

    void move_blk_to_end(blknum_t blknum){
        //already back case
        //no operation needed
        if(blknum == BM::ctrl_blk_list_back) return;

        ctrl_blk* cnt_blk = &BM::ctrl_blk_list[blknum]; //get block in list

        //if current block is at front of LRU list
        //set front to next pointer
        if(blknum == ctrl_blk_list_front){
            ctrl_blk_list_front = cnt_blk->lru_nxt_blk_number;
        }

        //connect their neighbors
        if(cnt_blk->lru_prv_blk_number >= 0) BM::ctrl_blk_list[cnt_blk->lru_prv_blk_number].lru_nxt_blk_number = cnt_blk->lru_nxt_blk_number;
        if(cnt_blk->lru_nxt_blk_number >= 0) BM::ctrl_blk_list[cnt_blk->lru_nxt_blk_number].lru_prv_blk_number = cnt_blk->lru_prv_blk_number;

        //append to end of list
        cnt_blk->lru_prv_blk_number = ctrl_blk_list_back;
        cnt_blk->lru_nxt_blk_number = BM::ctrl_blk_list[ctrl_blk_list_back].lru_nxt_blk_number;
        BM::ctrl_blk_list[ctrl_blk_list_back].lru_nxt_blk_number = blknum;

        //set end to given number
        BM::ctrl_blk_list_back = blknum;
    }

    ctrl_blk* get_ctrl_blk_from_buffer(int64_t table_id, pagenum_t pagenum){
        //find ctrl block in the list by using hash table
        blknum_t cnt_blk = BM::find_ctrl_blk_in_hash_table(table_id, pagenum);
        ctrl_blk* ret_blk; //return value
        if(cnt_blk != -1){
            //found case
            //get block from list
            ret_blk = &BM::ctrl_blk_list[cnt_blk];
        }
        else{
            //not found case
            //need eviction for space

            //get victim block number
            cnt_blk = BM::find_victim_blk_from_buffer();

            if(cnt_blk == -1){
                //can't get victim block
                //can't evict page so can't get such block
                throw "can't evict page from buffer";
            }

            //get block from list
            ret_blk = &BM::ctrl_blk_list[cnt_blk];
            
            if(ret_blk->is_dirty){
                //flush changes to disk if needed
                BM::flush_frame_to_file(cnt_blk);
            }

            //update hash table
            BM::hash_table.erase({ret_blk->table_id, ret_blk->pagenum});
            BM::hash_table[{table_id, pagenum}] = cnt_blk;
            
            //init block info
            ret_blk->pagenum = pagenum;
            ret_blk->table_id = table_id;
            ret_blk->is_dirty = ret_blk->is_pinned = 0;
            
            //read page from disk by call DSM api
            file_read_page(table_id, pagenum, ret_blk->frame_ptr);
        }
        //update LRU list
        BM::move_blk_to_end(cnt_blk);

        return ret_blk;
    }

}

//allocate the buffer pool with the given number of entries
int init_buffer(int num_buf){
    BM::BUFFER_SIZE = num_buf; //set buffer size

    //init list
    BM::frame_list = new frame_t[num_buf];
    BM::ctrl_blk_list = new BM::ctrl_blk[num_buf];

    for(int i=0; i<num_buf; i++){
        //point corresponding frame and connect neighbor control block
        //with prev and next block number
        //set -1 if not existed
        memset(BM::ctrl_blk_list + i,0,sizeof(BM::ctrl_blk));
        BM::ctrl_blk_list[i].frame_ptr = &BM::frame_list[i];
        BM::ctrl_blk_list[i].lru_nxt_blk_number = i+1 < num_buf ? i+1 : -1;
        BM::ctrl_blk_list[i].lru_prv_blk_number = i-1;
    }

    //set front and back in the list
    BM::ctrl_blk_list_front = 0;
    BM::ctrl_blk_list_back = num_buf - 1;

    //init hash table
    BM::hash_table.clear();

    return 0;
}

// Open existing table file or create one if it doesn't exist
int64_t buffer_open_table_file(const char* pathname){
    //just pass to DSM api call
    return file_open_table_file(pathname);
}

// Allocate a page
pagenum_t buffer_alloc_page(int64_t table_id){
    BM::ctrl_blk *header_blk, *nxt_blk;
    BM::_bm_page_t *header_page, *nxt_page;

    header_blk = BM::get_ctrl_blk_from_buffer(table_id, 0); //get header page block
    header_blk->is_pinned ++;

    header_page = reinterpret_cast<BM::_bm_page_t*>(header_blk->frame_ptr);
    
    uint64_t nxt_page_number = header_page->_header_page.free_page_number;

    if(nxt_page_number){
        //exist free page in list

        //read free page
        nxt_blk = BM::get_ctrl_blk_from_buffer(table_id, nxt_page_number);
        nxt_blk->is_pinned ++;

        nxt_page = reinterpret_cast<BM::_bm_page_t*>(nxt_blk->frame_ptr);

        //make header page to point next free page in list
        header_page->_header_page.free_page_number = nxt_page->_free_page.nxt_free_page_number;
        //init new page
        BM::init_free_page(&nxt_page->_raw_frame,0);

        //write changes in header page
        header_blk->is_dirty = true;
        header_blk->is_pinned --;
    }
    else{
        //no free page in list (db file grow occured)

        //get current number of page
        uint64_t current_number_of_pages = header_page->_header_page.number_of_pages;
        //make new pages as much as current number of page
        size_t num_of_new_pages = current_number_of_pages;
        
        //set to be allocated page to position of first new page 
        nxt_page_number = current_number_of_pages;

        //init header page to point second new page and grow page size twice
        header_page->_header_page.free_page_number = current_number_of_pages + 1;
        header_page->_header_page.number_of_pages <<= 1;

        //init free page lists
        //wipe out the first page before return the page number
        //init second page to last page to point next free page
        for(uint64_t i=0;i<num_of_new_pages;i++){
            //init free page and save changes
            //use 0 when need for wipe or indicate end of list
            //use current_number_of_pages + i + 1 to point next free page
            //this make linked free page list sequentially
            nxt_blk = BM::get_ctrl_blk_from_buffer(table_id, current_number_of_pages + i);
            nxt_blk->is_pinned ++;

            nxt_page = reinterpret_cast<BM::_bm_page_t*>(nxt_blk->frame_ptr);

            BM::init_free_page(&nxt_page->_raw_frame, (i==0||i+1>=num_of_new_pages?0:current_number_of_pages+i+1));
            
            nxt_blk->is_dirty = true;
            nxt_blk->is_pinned --;
        }

        nxt_blk = BM::get_ctrl_blk_from_buffer(table_id, nxt_page_number);
        nxt_blk->is_pinned ++;

        //save header changes after making new free page list
        header_blk->is_dirty = true;
        header_blk->is_pinned --;
    }

    return nxt_page_number;
}

// Free a page
void buffer_free_page(int64_t table_id, pagenum_t pagenum){
    //check pagenum is valid
    if(!BM::is_pagenum_valid(table_id, pagenum)){
        throw "pagenum is out of bound in buffer_free_page";
    }
    //check pagenum is header page
    if(!pagenum){
        throw "free header page";
    }
    
    BM::ctrl_blk *header_blk, *nxt_blk;
    BM::_bm_page_t *header_page, *nxt_page;

    header_blk = BM::get_ctrl_blk_from_buffer(table_id, 0); //get header page block
    header_blk->is_pinned ++;

    header_page = reinterpret_cast<BM::_bm_page_t*>(header_blk->frame_ptr);

    //read free page to return
    nxt_blk = BM::get_ctrl_blk_from_buffer(table_id, pagenum);

    nxt_page = reinterpret_cast<BM::_bm_page_t*>(nxt_blk->frame_ptr);

    //put the given page in the beginning of the list (LIFO)
    //init new page to point header page's nxt free page value
    BM::init_free_page(&nxt_page->_raw_frame, header_page->_header_page.free_page_number);
    //make header page to point given page number
    header_page->_header_page.free_page_number = pagenum;

    //write changes
    header_blk->is_dirty = true;
    header_blk->is_pinned --;
    nxt_blk->is_dirty = true;
    nxt_blk->is_pinned --;
}

// get a page pointer from buffer
void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest, bool readonly){
    //check pagenum is valid
    if(!BM::is_pagenum_valid(table_id, pagenum)){
        throw "pagenum is out of bound in buffer_read_page";
    }
    //get block from buffer
    BM::ctrl_blk* ret_blk = BM::get_ctrl_blk_from_buffer(table_id,pagenum);
    if(!readonly){
        //it can be written soon
        if(ret_blk->is_pinned){
            //already pinned to be written
            //can't write simultaneously
            throw "double write access";
        }
        ret_blk->is_pinned ++; //set pin
    }
    //copy page content to dest
    memcpy(dest,ret_blk->frame_ptr,sizeof(page_t));
    return;
}

// write a page to buffer
void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src){
    //check pagenum is valid
    if(!BM::is_pagenum_valid(table_id, pagenum)){
        throw "pagenum is out of bound in buffer_write_page";
    }
    //get block from buffer
    BM::ctrl_blk* ret_blk = BM::get_ctrl_blk_from_buffer(table_id,pagenum);
    //copy page content to dest
    memcpy(ret_blk->frame_ptr,src,sizeof(page_t));
    ret_blk->is_dirty = true; //set dirty bit on
    ret_blk->is_pinned --; //set unpin
}


// Flush all and destroy
void buffer_close_table_file(){
    for(size_t i=0; i<BM::BUFFER_SIZE; i++){
        //scan all block in buffer list
        if(BM::ctrl_blk_list[i].is_dirty){
            //flush dirty page only
            BM::flush_frame_to_file(i);
        }
    }

    //free the list
    delete[] BM::ctrl_blk_list;
    delete[] BM::frame_list;

    //clear the hash table
    BM::hash_table.clear();
}

