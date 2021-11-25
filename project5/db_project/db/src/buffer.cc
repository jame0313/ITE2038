#include "buffer.h"
#include <pthread.h>

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

    //buffer manager latch
    pthread_mutex_t buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

    //code by boost lib
    // https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
    template <class T1, class T2>
    size_t BM::hash_pair::operator()(const std::pair<T1, T2>& p) const {
        auto hash1 = std::hash<T1>{}(p.first);
        auto hash2 = std::hash<T2>{}(p.second);
        //use magic number and some bit shift to fit hash feature
        return hash1 ^ hash2 + 0x9e3779b9 + (hash2<<6) + (hash2>>2);
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
            //return corresponding block number
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
        bool is_acquired = false; //check whether find unlocked page
        while(cnt_blk != -1){
            //try to lock current page
            int status_code = pthread_mutex_trylock(&BM::ctrl_blk_list[cnt_blk].page_latch);
            if(!status_code){
                //acquired current blk
                is_acquired = true;
                break;
            }
            //get nxt LRU block
            //since current block is pinned (can't evict)
            cnt_blk = BM::ctrl_blk_list[cnt_blk].lru_nxt_blk_number;
        }
        if(is_acquired){
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
            //unlock to be evicted page
            pthread_mutex_unlock(&ret_blk->page_latch);
            
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
            ret_blk->is_dirty = 0;
            
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

    BM::buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

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

// Allocate a page
pagenum_t buffer_alloc_page(int64_t table_id){
    //read new page
    pagenum_t nxt_page_number = file_alloc_page(table_id);
    
    //load new page
    BM::ctrl_blk* nxt_blk = BM::get_ctrl_blk_from_buffer(table_id, nxt_page_number);
    //nxt_blk->is_pinned ++;

    return nxt_page_number;
}

// Free a page
void buffer_free_page(int64_t table_id, pagenum_t pagenum){
    BM::ctrl_blk* cnt_blk = BM::get_ctrl_blk_from_buffer(table_id, pagenum);
    cnt_blk->is_dirty = 0; //wipe block to be freed
    return file_free_page(table_id, pagenum);
}

// read a page from buffer
void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest, bool readonly){
    int status_code; //check for pthread error

    //start cirtical section
    status_code = pthread_mutex_lock(&BM::buffer_manager_latch);
    if(status_code) throw "pthread error occurred";

    //get block from buffer
    BM::ctrl_blk* ret_blk = BM::get_ctrl_blk_from_buffer(table_id,pagenum);
    
    if(!readonly){
        //lock when there will be modification
        status_code = pthread_mutex_trylock(&ret_blk->page_latch);
        if(status_code){
            //already pinned to be written
            //can't write simultaneously
            throw "double write access";
        }
    }
    //copy page content to dest
    memcpy(dest,ret_blk->frame_ptr,sizeof(page_t));

    //end cirtical section
    status_code = pthread_mutex_unlock(&BM::buffer_manager_latch);
    if(status_code) throw "pthread error occurred";
    return;
}

// write a page to buffer
void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src){
    int status_code; //check for pthread error

    //start cirtical section
    status_code = pthread_mutex_lock(&BM::buffer_manager_latch);
    if(status_code) throw "pthread error occurred";

    //get block from buffer
    BM::ctrl_blk* ret_blk = BM::get_ctrl_blk_from_buffer(table_id,pagenum);
    
    if(!pthread_mutex_trylock(&ret_blk->page_latch)){
        //not pinned to be written
        pthread_mutex_unlock(&ret_blk->page_latch);
        throw "invalid write access";
    }

    if(src){
        //copy page content to dest
        memcpy(ret_blk->frame_ptr,src,sizeof(page_t));
        ret_blk->is_dirty = true; //set dirty bit on
    }

    pthread_mutex_unlock(&ret_blk->page_latch); //unlock current page

    //end cirtical section
    status_code = pthread_mutex_unlock(&BM::buffer_manager_latch);
    if(status_code) throw "pthread error occurred";
    return;
}

// Flush all and destroy
void buffer_close_table_file(){
    pthread_mutex_lock(&BM::buffer_manager_latch);

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

    pthread_mutex_unlock(&BM::buffer_manager_latch);
}

void buffer_close_table_file(int64_t table_id){
    pthread_mutex_lock(&BM::buffer_manager_latch);

    for(size_t i=0; i<BM::BUFFER_SIZE; i++){
        //scan all block in buffer list
        if(BM::ctrl_blk_list[i].is_dirty && BM::ctrl_blk_list[i].table_id == table_id){
            //flush dirty page only
            BM::ctrl_blk_list[i].is_dirty = 0;
            BM::flush_frame_to_file(i);
        }
    }

    pthread_mutex_unlock(&BM::buffer_manager_latch);
}