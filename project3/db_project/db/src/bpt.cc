#include "bpt.h"

namespace FIM{
    pagenum_t make_page(int64_t table_id){
        //get new page from BM
        pagenum_t x = buffer_alloc_page(table_id);
        return x;
    }

    int change_root_page(int64_t table_id, pagenum_t root_page_number, bool del_tree_flag){
        _fim_page_t header_page;
        
        if(!del_tree_flag && !root_page_number){
            //you can't set header page number unless indicate the tree is to vanish
            perror("invalid root page number");
            return -1;
        }

        try{
            //set root page in header page
            buffer_read_page(table_id, 0, &header_page._raw_page);
            header_page._header_page.root_page_number = root_page_number;
            buffer_write_page(table_id, 0, &header_page._raw_page);
            return 0;
        }
        catch(const char* e){
            perror(e);
            return -1;
        }
    }

    pagenum_t find_leaf_page(int64_t table_id, int64_t key){
        _fim_page_t header_page, cnt_page;

        //get header page to get root page number
        buffer_read_page(table_id,0,&header_page._raw_page, true);
        pagenum_t root = header_page._header_page.root_page_number; //root page number

        if(!root) return 0; //no tree case

        //current page(return value)
        pagenum_t cnt_page_number = root;
        buffer_read_page(table_id,root,&cnt_page._raw_page, true);
        
        //find while current page is leaf page
        //find child page x where x th page's key <= key < x+1 th page's key
        while(!cnt_page._leaf_page.page_header.is_leaf){
            pagenum_t pre_page_number = cnt_page_number;

            uint32_t num_keys = cnt_page._internal_page.page_header.number_of_keys;

            if(key < cnt_page._internal_page.key_and_page[0].key){
                //leftmost page case
                cnt_page_number = cnt_page._internal_page.leftmost_page_number;
            }
            else{
                for(uint32_t i = 0; i < num_keys; i++){
                    if(key < cnt_page._internal_page.key_and_page[i].key){
                        //middle page case
                        cnt_page_number = cnt_page._internal_page.key_and_page[i-1].page_number;
                        break;
                    }
                    else if(i+1 == num_keys){
                        //rightmost page case
                        cnt_page_number = cnt_page._internal_page.key_and_page[i].page_number;
                    }
                }
            }
            if(pre_page_number == cnt_page_number){
                //not updated, tree malstructed
                throw "inf loop in find leaf page";
            }
            //get next page
            buffer_read_page(table_id,cnt_page_number,&cnt_page._raw_page, true);
        }
        return cnt_page_number;
    }

    int find_record(int64_t table_id, int64_t key, char *ret_val, uint16_t* val_size){
        
        //find leaf page
        pagenum_t leaf_page_number = FIM::find_leaf_page(table_id,key);
        if(!leaf_page_number) return -1; //can't find leaf page

        _fim_page_t leaf_page;
        buffer_read_page(table_id,leaf_page_number,&leaf_page._raw_page, true);

        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;

        for(uint32_t i = 0; i < num_keys; i++){
            if(leaf_page._leaf_page.slot[i].key == key){
                //find record
                if(ret_val){
                    //push record value when ret_val is not NULL
                    *val_size = leaf_page._leaf_page.slot[i].size;
                    memcpy(ret_val,leaf_page._raw_page.raw_data+(leaf_page._leaf_page.slot[i].offset),*val_size);
                }
                return 0;
            }
        }
        return -1; //can't find record
    }

    int insert_record(int64_t table_id, int64_t key, char *value, uint16_t val_size){
        
        if(!FIM::find_record(table_id,key)) return -1; //there is key in tree already

        _fim_page_t header_page, leaf_page;
        buffer_read_page(table_id,0,&header_page._raw_page, true); //get header page to get root page
        pagenum_t root = header_page._header_page.root_page_number;

        if(!root){
            //no tree case
            //make new tree
            root = FIM::init_new_tree(table_id, key, value, val_size);
            return FIM::change_root_page(table_id, root);
        }

        //find corresponding leaf page to insert record
        pagenum_t leaf_page_number = FIM::find_leaf_page(table_id,key);
        buffer_read_page(table_id,leaf_page_number,&leaf_page._raw_page, true);

        //check insert operation's result need splitting
        uint64_t left_space = leaf_page._leaf_page.amount_of_free_space;
        bool is_split_needed = val_size + sizeof(FIM::page_slot_t) > left_space;

        if(!is_split_needed){
            //enough free space to insert
            //just insert and end operation
            FIM::insert_into_leaf_page(leaf_page_number, table_id, key, value, val_size);
            return 0;
        }
        else{
            //no room for the new record
            //split the leaf page
            return FIM::insert_into_leaf_page_after_splitting(leaf_page_number, table_id, key, value, val_size);
        }
    }

    pagenum_t init_new_tree(int64_t table_id, int64_t key, char *value, uint16_t val_size){
        pagenum_t root = FIM::make_page(table_id); //get new page
        _fim_page_t root_page = {0,};
        
        //set init state
        root_page._leaf_page.page_header.parent_page_number = 0;
        root_page._leaf_page.page_header.is_leaf = 1; //set leaf
        root_page._leaf_page.page_header.number_of_keys = 1;
        root_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        root_page._leaf_page.amount_of_free_space -= val_size + sizeof(FIM::page_slot_t);
        root_page._leaf_page.right_sibling_page_number = 0;
        
        //insert given key and value
        root_page._leaf_page.slot[0].key = key;
        root_page._leaf_page.slot[0].offset = PAGE_SIZE - val_size;
        root_page._leaf_page.slot[0].size = val_size;
        memcpy(root_page._raw_page.raw_data + (PAGE_SIZE - val_size), value, val_size);
        
        //save changes
        buffer_write_page(table_id,root,&root_page._raw_page);
        return root;
    }

    void insert_into_leaf_page(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size){
        _fim_page_t leaf_page;
        buffer_read_page(table_id,leaf_page_number,&leaf_page._raw_page); //get leaf page
        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;

        //set new offset
        //place right free space(next to last value) to remain packed
        uint16_t offset = PAGE_SIZE;
        for(uint32_t i = 0; i < num_keys; i++) offset = std::min(offset, leaf_page._leaf_page.slot[i].offset);
        offset -= val_size;

        FIM::page_slot_t new_slot = {key, val_size, offset}; //new slot info to insert

        //check new key needs to inserted in rightmost slot
        bool inserted_in_page = false;
        
        //find record x index where x-1 th record's key < key < x th record's key
        for(uint32_t i = 0; i < num_keys; i++){
            if(key < leaf_page._leaf_page.slot[i].key){

                //shift right slot to make space
                for(uint32_t j = num_keys; j > i; j--){
                    leaf_page._leaf_page.slot[j] = leaf_page._leaf_page.slot[j-1];
                }

                //store new slot in page
                leaf_page._leaf_page.slot[i] = new_slot;
                inserted_in_page = true;
                break;
            }
        }

        if(!inserted_in_page){
            //rightmost key case
            leaf_page._leaf_page.slot[num_keys] = new_slot;
        }

        memcpy(leaf_page._raw_page.raw_data + offset, value, val_size); //store value in page

        leaf_page._leaf_page.amount_of_free_space -= val_size + sizeof(FIM::page_slot_t); //update free space
        leaf_page._leaf_page.page_header.number_of_keys ++; //update key number
        buffer_write_page(table_id,leaf_page_number,&leaf_page._raw_page); //save changes
        return;
    }

    int insert_into_leaf_page_after_splitting(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size){
        pagenum_t new_leaf_page_number = FIM::make_page(table_id); //get new page (new leaf)
        _fim_page_t new_leaf_page = {0,}, leaf_page;
        
        new_leaf_page._leaf_page.page_header.is_leaf = 1; //set leaf
        
        buffer_read_page(table_id,leaf_page_number,&leaf_page._raw_page); //get leaf page (old leaf)
        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;
        
        //temp slot and value to store all record
        page_slot_t *tmp_slot = new page_slot_t[num_keys+1];
        char** value_list = new char* [num_keys+1];

        //insertion point to set new slot
        //new slot should set insert_point-th slot
        uint32_t insert_point = num_keys;

        for(uint32_t i = 0; i < num_keys; i++){
            if(key < leaf_page._leaf_page.slot[i].key){
                //get insertion point
                insert_point = i;
                break;
            }
        }

        //make temp slot and value list
        for(uint32_t i = 0, j = 0; j < num_keys + 1; i++,j++){
            if(i == insert_point){
                //store new key and value
                tmp_slot[j].key = key;
                tmp_slot[j].size = val_size;
                
                value_list[j] = new char[val_size+1];
                memcpy(value_list[j],value,val_size);
                value_list[j][val_size] = 0; //use for safety
                j++;
            }
            if(j < num_keys + 1){
                //store leaf page's slot
                tmp_slot[j] = leaf_page._leaf_page.slot[i];

                value_list[j] = new char[tmp_slot[j].size+1];
                memcpy(value_list[j],leaf_page._raw_page.raw_data+(tmp_slot[j].offset),tmp_slot[j].size);
                value_list[j][tmp_slot[j].size] = 0; //use for safety
            }
        }

        //cumulative sum of left page's contents
        uint32_t cnt_size_sum = 0; 
        
        //limit left page size
        //set left page size smaller than threshold
        uint16_t threshold = (PAGE_SIZE - PAGE_HEADER_SIZE) >> 1;

        //split point in tmp slot and value list
        //left page store [0,split_point) slot
        //right page store [split_point, num_keys] slot
        uint32_t split_point = 0;

        for(uint32_t i = 0; i < num_keys + 1; i++){
            uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t); //ith slot size
            if(cnt_size_sum + cnt_size >= threshold){
                //find split point
                split_point = i;
                break;
            }
            cnt_size_sum += cnt_size; //add current size
        }

        //init free space
        leaf_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        new_leaf_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;

        for(uint32_t i = 0; i < split_point; i++){
            //push slot in left page
            tmp_slot[i].offset = (i>0?leaf_page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size; //calc new offset
            leaf_page._leaf_page.slot[i] = tmp_slot[i];
            memcpy(leaf_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            leaf_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t); //update free space
        }

        for(uint32_t i = split_point; i < num_keys + 1; i++){
            //push slot in right page
            uint32_t j = i - split_point;
            tmp_slot[i].offset = (j>0?new_leaf_page._leaf_page.slot[j-1].offset:PAGE_SIZE) - tmp_slot[i].size; //calc new offset
            new_leaf_page._leaf_page.slot[j] = tmp_slot[i];
            memcpy(new_leaf_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            new_leaf_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t); //update free space
        }

        //delete temp array
        delete[] tmp_slot;
        for(int i = 0; i < num_keys + 1; i++) delete[] value_list[i];
        delete[] value_list;

        //set parent, # of keys, sibling
        new_leaf_page._leaf_page.page_header.parent_page_number =
        leaf_page._leaf_page.page_header.parent_page_number;

        leaf_page._leaf_page.page_header.number_of_keys = split_point;
        new_leaf_page._leaf_page.page_header.number_of_keys = num_keys + 1 - split_point;
        
        new_leaf_page._leaf_page.right_sibling_page_number =
        leaf_page._leaf_page.right_sibling_page_number;
        leaf_page._leaf_page.right_sibling_page_number = new_leaf_page_number;
        
        //wipe free space in pages
        memset(leaf_page._raw_page.raw_data + PAGE_HEADER_SIZE + (leaf_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, leaf_page._leaf_page.amount_of_free_space);
        memset(new_leaf_page._raw_page.raw_data + PAGE_HEADER_SIZE + (new_leaf_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, new_leaf_page._leaf_page.amount_of_free_space);

        //save changes
        buffer_write_page(table_id,leaf_page_number,&leaf_page._raw_page);
        buffer_write_page(table_id,new_leaf_page_number,&new_leaf_page._raw_page);

        //get new key from right page's first key
        int64_t new_key = new_leaf_page._leaf_page.slot[0].key;

        //need to insert new key and new page in parent page
        return FIM::insert_into_parent_page(leaf_page_number, table_id, new_key, new_leaf_page_number);
    }

    int insert_into_parent_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        _fim_page_t left_page, parent_page;
        buffer_read_page(table_id,left_page_number,&left_page._raw_page, true); //get left page to get parent page number

        pagenum_t parent_page_number = left_page._internal_page.page_header.parent_page_number;
        
        if(!parent_page_number){
            //left page is root page case
            //make new root page to connect left and right page
            pagenum_t root =  FIM::insert_into_new_root_page(left_page_number, table_id, key, right_page_number);
            return FIM::change_root_page(table_id, root);
        }

        buffer_read_page(table_id,parent_page_number,&parent_page._raw_page, true); //get parent page

        uint32_t num_keys = parent_page._internal_page.page_header.number_of_keys;

        //check parent page should split
        bool is_split_needed = num_keys + 1 > MAX_KEY_NUMBER; 

        if(!is_split_needed){
            //enough space to insert
            //just insert key and right page and end operation
            FIM::insert_into_page(parent_page_number, left_page_number, table_id, key, right_page_number);
            return 0;
        }
        else{
            //no room for new key (parent page has full key)
            //split the parent page
            return FIM::insert_into_page_after_splitting(parent_page_number, left_page_number, table_id, key, right_page_number);
        }
    }

    pagenum_t insert_into_new_root_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        pagenum_t root = FIM::make_page(table_id); //get new root page
        _fim_page_t root_page = {0,}, left_page, right_page;

        //get left and right page
        buffer_read_page(table_id,left_page_number,&left_page._raw_page);
        buffer_read_page(table_id,right_page_number,&right_page._raw_page);
        
        //set root page
        root_page._internal_page.page_header.parent_page_number = 0;
        root_page._internal_page.page_header.number_of_keys = 1;

        //insert left, right page and middle key
        root_page._internal_page.leftmost_page_number = left_page_number;
        root_page._internal_page.key_and_page[0].key = key;
        root_page._internal_page.key_and_page[0].page_number = right_page_number;

        //connect two pages into root page as parent
        left_page._internal_page.page_header.parent_page_number = root;
        right_page._internal_page.page_header.parent_page_number = root;

        //save changes
        buffer_write_page(table_id,root,&root_page._raw_page);
        buffer_write_page(table_id,left_page_number,&left_page._raw_page);
        buffer_write_page(table_id,right_page_number,&right_page._raw_page);

        //return new root page number
        return root;
    }
    
    void insert_into_page(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        _fim_page_t page;
        buffer_read_page(table_id,page_number,&page._raw_page); //get page
        
        uint32_t num_keys = page._internal_page.page_header.number_of_keys;
        int left_index = -1; //left child page's index in parent page

        //find left index
        for(uint32_t i=0; i<num_keys; i++){
            if(page._internal_page.key_and_page[i].page_number == left_page_number){
                left_index = i;
                break;
            }
        }

        //need to insert key and right page next to left page
        left_index ++;

        //shift right pages to make space
        for(uint32_t i = num_keys; i>left_index; i--){
            page._internal_page.key_and_page[i] = page._internal_page.key_and_page[i-1];
        }

        //insert new key and page
        page._internal_page.key_and_page[left_index].key = key;
        page._internal_page.key_and_page[left_index].page_number = right_page_number;
        
        page._internal_page.page_header.number_of_keys ++;

        //save changes
        buffer_write_page(table_id,page_number,&page._raw_page);
        return;
    }

    int insert_into_page_after_splitting(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        pagenum_t new_page_number = FIM::make_page(table_id); //get new page (new internal page)
        _fim_page_t new_page = {0,}, page, tmp_page;
        
        buffer_read_page(table_id,page_number,&page._raw_page); //get old page
        uint32_t num_keys = page._internal_page.page_header.number_of_keys;
        
        //temp array to sort key and page number in page and new key and page number
        pagenum_t tmp_leftmost_page_number = page._internal_page.leftmost_page_number;
        keypagenum_pair_t *tmp_pair = new keypagenum_pair_t[num_keys+1];

        //insertion point to set new key and page num
        //new data should set insert_point-th key
        int insert_point = -1; 

        for(uint32_t i = 0; i < num_keys; i++){
            if(page._internal_page.key_and_page[i].page_number == left_page_number){
                //get left page's index
                insert_point = i;
                break;
            }
        }
        
        //set new data next to left page
        insert_point ++;

        //make temp key and page number list
        for(uint32_t i = 0, j = 0; j < num_keys + 1; i++,j++){
            if(i == insert_point){
                //store new pair
                tmp_pair[j].key = key;
                tmp_pair[j].page_number = right_page_number;
                j++;
            }
            if(j < num_keys + 1){
                //store page's content
                tmp_pair[j] = page._internal_page.key_and_page[i];
            }
        }

        //split point in array
        //left page store [0,split_point) pair
        //right page store [split_point + 1, num_keys] pair
        uint32_t split_point = DEFAULT_ORDER;
        
        for(uint32_t i = 0; i < split_point; i++){
            //push data in left page
            page._internal_page.key_and_page[i] = tmp_pair[i];
        }

        //set new key(used in page's parent page insertion) as middle key
        //middle key is in split_point-th key
        //save split_point-th page in right page
        int64_t new_key = tmp_pair[split_point].key;
        new_page._internal_page.leftmost_page_number = tmp_pair[split_point].page_number;
        
        for(uint32_t i = split_point + 1; i < num_keys + 1; i++){
            //push data in right page
            uint32_t j = i - (split_point + 1);
            new_page._internal_page.key_and_page[j] = tmp_pair[i];
        }

        //delete temp array
        delete[] tmp_pair;

        //set parent, # of keys
        new_page._internal_page.page_header.parent_page_number =
        page._internal_page.page_header.parent_page_number;

        page._internal_page.page_header.number_of_keys = split_point;
        new_page._internal_page.page_header.number_of_keys = split_point;

        //find free space start point
        int64_t offset = PAGE_HEADER_SIZE + split_point*(sizeof(FIM::keypagenum_pair_t));

        //wipe free space
        memset(page._raw_page.raw_data + offset, 0, PAGE_SIZE - offset);
        memset(new_page._raw_page.raw_data + offset, 0, PAGE_SIZE - offset);
        
        //save changes
        buffer_write_page(table_id,page_number,&page._raw_page);
        buffer_write_page(table_id,new_page_number,&new_page._raw_page);

        //update right page's children pages to point right page as parent
        buffer_read_page(table_id,new_page._internal_page.leftmost_page_number,&tmp_page._raw_page);
        tmp_page._internal_page.page_header.parent_page_number = new_page_number;
        buffer_write_page(table_id,new_page._internal_page.leftmost_page_number,&tmp_page._raw_page);
        
        for(uint32_t i = 0; i < split_point; i++){
            buffer_read_page(table_id,new_page._internal_page.key_and_page[i].page_number,&tmp_page._raw_page);
            tmp_page._internal_page.page_header.parent_page_number = new_page_number;
            buffer_write_page(table_id,new_page._internal_page.key_and_page[i].page_number,&tmp_page._raw_page);
        }

        //need to insert new key and new page in parent page
        return FIM::insert_into_parent_page(page_number, table_id, new_key, new_page_number);
    }

    int delete_record(int64_t table_id, int64_t key){

        if(FIM::find_record(table_id,key) != 0) return -1; //there is no such key in tree

        pagenum_t leaf_page = FIM::find_leaf_page(table_id, key); //get leaf page where the given key is

        //delete key and data in leaf page
        return FIM::delete_entry(leaf_page, table_id, key);
    }

    int delete_entry(pagenum_t page_number, int64_t table_id, int64_t key){
        _fim_page_t page, header_page, parent_page, neighbor_page;
        
        FIM::remove_entry_from_page(page_number, table_id, key); //remove key and data in page

        buffer_read_page(table_id,page_number,&page._raw_page, true); //get result page

        pagenum_t parent_page_number = page._internal_page.page_header.parent_page_number;
        
        if(!parent_page_number){
            //current page is root page case
            //need to check specially such as empty root or empty tree case

            //get new root page number or indicate 0 as there is no tree
            pagenum_t root = FIM::adjust_root_page(page_number, table_id);

            //if root is 0, set del tree flag to explicitly set empty tree
            return FIM::change_root_page(table_id, root, root == 0);
        }

        //check delete operation's result need modify
        //there is two cases(leaf pages and internal pages)
        //use different criteria depending on page type
        bool is_modify_needed;
        uint64_t maximum_free_space = MAX_FREE_SPACE;
        uint32_t min_keys = DEFAULT_ORDER;
        uint32_t is_leaf = page._leaf_page.page_header.is_leaf;

        if(is_leaf){
            //leaf page case
            //check current page has too much free space that page should be modified
            is_modify_needed = page._leaf_page.amount_of_free_space >= maximum_free_space;
        }
        else{
            //internal page case
            //check current page is violated occupancy invariant
            is_modify_needed = page._internal_page.page_header.number_of_keys < DEFAULT_ORDER;
        }

        if(is_modify_needed){
            //need to modify case
            //need to merge two pages into one page
            //or redistribute two pages' contents
            //to meet invariant in two pages

            buffer_read_page(table_id,parent_page_number,&parent_page._raw_page, true); //get parent page to find neightbor page

            //try to find left neighbor and key between two pages
            pagenum_t neighbor_page_number = 0;
            int64_t middle_key;
            bool is_leftmost = false; // there is no left neighbor page (current page is leftmost page)

            if(parent_page._internal_page.leftmost_page_number == page_number){
                //current page is leftmost page case
                //use right neighbor page

                is_leftmost = true; //set flag

                //find right neighbor page
                middle_key = parent_page._internal_page.key_and_page[0].key;
                neighbor_page_number = parent_page._internal_page.key_and_page[0].page_number;
            }
            else{
                //current page is not leftmost page case

                //find left neighbor page
                for(uint32_t i=0; i<parent_page._internal_page.page_header.number_of_keys; i++){
                    if(parent_page._internal_page.key_and_page[i].page_number == page_number){
                        middle_key = parent_page._internal_page.key_and_page[i].key;

                        neighbor_page_number = i>0?
                        parent_page._internal_page.key_and_page[i-1].page_number:
                        parent_page._internal_page.leftmost_page_number;
                        break;
                    }
                }
            }

            if(!neighbor_page_number){
                //can't find neighbor page
                //the tree is malstructed
                perror("can't find neighbor page");
                return -1;
            }

            buffer_read_page(table_id,neighbor_page_number,&neighbor_page._raw_page, true); //get neighbor page
            bool can_merge; //check two pages can merge into one page

            if(is_leaf){
                //leaf page case
                //sum of two page's free space should large enough to fit total contents in single page
                can_merge = page._leaf_page.amount_of_free_space + neighbor_page._leaf_page.amount_of_free_space >= (PAGE_SIZE - PAGE_HEADER_SIZE);
            }
            else{
                //internal page case
                //sum of two page's key + 1 should obey key invariant
                can_merge = page._internal_page.page_header.number_of_keys + neighbor_page._internal_page.page_header.number_of_keys + 1 <= MAX_KEY_NUMBER;
            }

            if(can_merge){
                //merge can occured case
                //merge two pages into one page
                return FIM::merge_pages(page_number, table_id, middle_key, neighbor_page_number, is_leftmost);
            }
            else{
                //can't merge case
                //need to redistribute two pages' content
                FIM::redistribute_pages(page_number, table_id, middle_key, neighbor_page_number, is_leftmost);
                return 0;
            }
        }
        else return 0; //no modification need case, just end operation
    }

    void remove_entry_from_page(pagenum_t page_number, uint64_t table_id, int64_t key){
        _fim_page_t page;
        buffer_read_page(table_id, page_number, &page._raw_page); //get page

        uint32_t num_keys = page._leaf_page.page_header.number_of_keys;
        
        if(page._leaf_page.page_header.is_leaf){
            //leaf page case

            //temp slot and value list to store records
            page_slot_t *tmp_slot = new page_slot_t[num_keys-1];
            char **value_list = new char* [num_keys-1];
            
            //make temp slot and value list
            for(uint32_t i=0, j=0; i<num_keys; i++, j++){
                if(page._leaf_page.slot[i].key == key){
                    //skip to be deleted record

                    //update free space
                    page._leaf_page.amount_of_free_space += page._leaf_page.slot[i].size + sizeof(FIM::page_slot_t);
                    i++;
                }
                if(i<num_keys){
                    //store page's slot
                    tmp_slot[j] = page._leaf_page.slot[i];

                    value_list[j] = new char [tmp_slot[j].size+1];
                    memcpy(value_list[j], page._raw_page.raw_data + (tmp_slot[j].offset), tmp_slot[j].size);
                    value_list[j][tmp_slot[j].size] = 0; //use for safety
                }
            }

            //push remained slot and data in page
            for(uint32_t i=0; i<num_keys-1 ;i++){
                tmp_slot[i].offset = (i>0?page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size;
                page._leaf_page.slot[i] = tmp_slot[i];
                memcpy(page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            }

            //delete temp array
            delete[] tmp_slot;
            for(int i = 0; i < num_keys - 1; i++) delete[] value_list[i];
            delete[] value_list;

            //wipe free space in page
            memset(page._raw_page.raw_data + PAGE_HEADER_SIZE + ((num_keys-1)*sizeof(FIM::page_slot_t)), 0, page._leaf_page.amount_of_free_space);

        }
        else{
            //internal page case

            //delete key and page number in same pair
            for(uint32_t i=0; i<num_keys; i++){
                if(page._internal_page.key_and_page[i].key == key){
                    //find to be deleted key

                    //shift left pair to fill space (to be deleted key space)
                    //remove page # right next to given key
                    //(i.e. key <= page's key range < next key)
                    for(uint32_t j=i+1; j<num_keys; j++){
                        page._internal_page.key_and_page[j-1] = page._internal_page.key_and_page[j];
                    }

                    //wipe free space in page
                    page._internal_page.key_and_page[num_keys - 1] = {0,0};
                    break;
                }
                
            }
        }
        page._leaf_page.page_header.number_of_keys --;
        buffer_write_page(table_id, page_number, &page._raw_page); //save changes
    }

    pagenum_t adjust_root_page(pagenum_t root_page_number, int64_t table_id){
        _fim_page_t root_page, new_root_page;
        buffer_read_page(table_id, root_page_number, &root_page._raw_page, true); //get current root
        
        uint32_t num_keys = root_page._leaf_page.page_header.number_of_keys;
        
        //root page still has key
        //no need to do additional operation
        if(num_keys > 0) return root_page_number;

        pagenum_t new_root_page_number;

        if(root_page._leaf_page.page_header.is_leaf){
            //root page was leaf page case
            //deleted the only one page left in tree
            //indicate the tree became empty
            new_root_page_number =  0;
        }
        else{
            //get root page's only child page (leftmost child)
            //make this page as new root page
            new_root_page_number = root_page._internal_page.leftmost_page_number;
            
            buffer_read_page(table_id, new_root_page_number, &new_root_page._raw_page); //get new root page
            new_root_page._leaf_page.page_header.parent_page_number = 0; //set page as root page
            buffer_write_page(table_id, new_root_page_number, &new_root_page._raw_page); //save changes
        }
        buffer_free_page(table_id, root_page_number); //free deleted root page

        return new_root_page_number; //return new root page or 0 to indicate empty tree
    }

    int merge_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost){
        _fim_page_t left_page, right_page, tmp_page;


        if(is_leftmost) std::swap(page_number, neighbor_page_number); //swap two page # to get left and right page #

        pagenum_t right_page_number = page_number;
        pagenum_t left_page_number = neighbor_page_number;

        if(is_leftmost) std::swap(page_number, neighbor_page_number); //swap again to restore status

        //get two pages
        buffer_read_page(table_id, right_page_number, &right_page._raw_page, true);
        buffer_read_page(table_id, left_page_number, &left_page._raw_page);

        uint32_t left_num_keys = left_page._leaf_page.page_header.number_of_keys;
        uint32_t right_num_keys = right_page._leaf_page.page_header.number_of_keys;

        if(left_page._leaf_page.page_header.is_leaf){
            //leaf page case

            //set new offset
            //place right free space(next to last value) in left space to remain packed
            uint16_t offset = PAGE_SIZE;
            for(uint32_t i = 0; i < left_num_keys; i++) offset = std::min(offset, left_page._leaf_page.slot[i].offset);
            
            //store all right page's contents in left page
            for(uint32_t i=0; i<right_num_keys; i++){
                uint32_t j = left_num_keys + i;
                left_page._leaf_page.slot[j] = right_page._leaf_page.slot[i]; //store slot

                left_page._leaf_page.slot[j].offset = (i > 0?
                    left_page._leaf_page.slot[j - 1].offset:
                    offset)
                    - right_page._leaf_page.slot[i].size; //set new offset

                memcpy(left_page._raw_page.raw_data + left_page._leaf_page.slot[j].offset,
                    right_page._raw_page.raw_data + right_page._leaf_page.slot[i].offset,
                    left_page._leaf_page.slot[j].size); //store value

                //update free space
                left_page._leaf_page.amount_of_free_space -= left_page._leaf_page.slot[j].size + sizeof(FIM::page_slot_t);
            }

            //set # of keys and sibling
            left_page._leaf_page.page_header.number_of_keys += right_num_keys;
            left_page._leaf_page.right_sibling_page_number = right_page._leaf_page.right_sibling_page_number;
        }
        else{
            //internal page case

            //set right page's children parent as left page in advance
            buffer_read_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);
            tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
            buffer_write_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);

            for(uint32_t i=0; i<right_num_keys; i++){
                buffer_read_page(table_id, right_page._internal_page.key_and_page[i].page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
                buffer_write_page(table_id, right_page._internal_page.key_and_page[i].page_number, &tmp_page._raw_page);
            }

            //store middle key also in left page
            //insert first pair (special case)
            left_page._internal_page.key_and_page[left_num_keys].key = middle_key;
            left_page._internal_page.key_and_page[left_num_keys].page_number =
            right_page._internal_page.leftmost_page_number;

            //store remaining contents in right page into left page
            for(uint32_t i=0; i<right_num_keys; i++){
                left_page._internal_page.key_and_page[left_num_keys + 1 + i] =
                right_page._internal_page.key_and_page[i];
            }

            //update # of keys
            left_page._leaf_page.page_header.number_of_keys += right_num_keys + 1;
        }

        //save changes ONLY in left page
        //free deleted right page
        buffer_write_page(table_id, left_page_number, &left_page._raw_page);
        buffer_free_page(table_id, right_page_number);

        pagenum_t parent_page_number = left_page._leaf_page.page_header.parent_page_number;
        
        //need to delete right page's key(middle key) and right page number in parent page
        return FIM::delete_entry(parent_page_number, table_id, middle_key);
    }

    void redistribute_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost){
        _fim_page_t left_page, right_page, tmp_page, parent_page;
        if(is_leftmost) std::swap(page_number, neighbor_page_number); //swap two page # to get left and right page #

        pagenum_t right_page_number = page_number;
        pagenum_t left_page_number = neighbor_page_number;

        if(is_leftmost) std::swap(page_number, neighbor_page_number); //swap again to restore status

        //get two pages
        buffer_read_page(table_id, right_page_number, &right_page._raw_page);
        buffer_read_page(table_id, left_page_number, &left_page._raw_page);

        uint32_t left_num_keys = left_page._leaf_page.page_header.number_of_keys;
        uint32_t right_num_keys = right_page._leaf_page.page_header.number_of_keys;
        int64_t new_middle_key; //new middle key to update parent page

        if(left_page._leaf_page.page_header.is_leaf){
            //leaf page case

            uint64_t left_free_space = left_page._leaf_page.amount_of_free_space;
            uint64_t right_free_space = right_page._leaf_page.amount_of_free_space;
            
            //temp slot and value to store all record in two pages
            page_slot_t *tmp_slot = new page_slot_t[left_num_keys+right_num_keys];
            char** value_list = new char* [left_num_keys+right_num_keys];

            //fill temp slot with left page's content
            for(uint32_t i=0; i<left_num_keys; i++){
                tmp_slot[i] = left_page._leaf_page.slot[i];
                value_list[i] = new char[tmp_slot[i].size+1];
                memcpy(value_list[i],left_page._raw_page.raw_data+(tmp_slot[i].offset),tmp_slot[i].size);
                value_list[i][tmp_slot[i].size] = 0;
            }

            //append temp slot with right page's content
            for(uint32_t j=0, i=left_num_keys; j<right_num_keys; i++, j++){
                tmp_slot[i] = right_page._leaf_page.slot[j];
                value_list[i] = new char[tmp_slot[i].size+1];
                memcpy(value_list[i],right_page._raw_page.raw_data+(tmp_slot[i].offset),tmp_slot[i].size);
                value_list[i][tmp_slot[i].size] = 0;
            }
            //cumulative sum of left or right page's contents
            uint16_t cnt_size_sum = 0;

            //limit left or right page size(upper bound)
            //set both pages free space smaller than threshold
            uint16_t threshold = MAX_FREE_SPACE;

            //split point in tmp slot and value list
            //left page store [0,split_point) slot
            //right page store [split_point, sum of two pages' # of keys] slot
            uint32_t split_point = 0;
            
            if(left_free_space>right_free_space){
                //left page need some records case

                //find split point in right page part
                for(uint32_t i = left_num_keys; i < left_num_keys + right_num_keys; i++){
                    uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t);
                    if(left_free_space - (cnt_size_sum + cnt_size) < threshold){
                        //find split point
                        split_point = i + 1;
                        break;
                    }
                    cnt_size_sum += cnt_size; //add current size
                }
            }
            else{
                //right page need some records case

                //find split point in left page part
                for(uint32_t i = left_num_keys - 1; i > 0; i--){
                    uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t);
                    if(right_free_space - (cnt_size_sum + cnt_size) < threshold){
                        //find split point
                        split_point = i;
                        break;
                    }
                    cnt_size_sum += cnt_size; //add current size
                }
            }

            //init free space
            left_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
            right_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;

            for(uint32_t i = 0; i < split_point; i++){
                //push slot in left page
                tmp_slot[i].offset = (i>0?left_page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size; //calc new offset
                left_page._leaf_page.slot[i] = tmp_slot[i];
                memcpy(left_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
                left_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t); //update free space
            }

            for(uint32_t i = split_point; i < left_num_keys + right_num_keys; i++){
                //push slot in right page
                uint32_t j = i-split_point;
                tmp_slot[i].offset = (j>0?right_page._leaf_page.slot[j-1].offset:PAGE_SIZE) - tmp_slot[i].size; //calc new offset
                right_page._leaf_page.slot[j] = tmp_slot[i];
                memcpy(right_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
                right_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t); //update free space
            }

            //delete temp array
            delete[] tmp_slot;
            for(int i = 0; i < left_num_keys+right_num_keys; i++) delete[] value_list[i];
            delete[] value_list;

            //update # of keys
            left_page._leaf_page.page_header.number_of_keys = split_point;
            right_page._leaf_page.page_header.number_of_keys = left_num_keys + right_num_keys - split_point;

            //wipe free space in pages
            memset(left_page._raw_page.raw_data + PAGE_HEADER_SIZE + (left_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, left_page._leaf_page.amount_of_free_space);
            memset(right_page._raw_page.raw_data + PAGE_HEADER_SIZE + (right_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, right_page._leaf_page.amount_of_free_space);
            
            //set new middle key as right page's first key
            new_middle_key = right_page._leaf_page.slot[0].key;
        }
        else{
            if(left_num_keys<right_num_keys){
                //left page need one more key case

                //append middle key and leftmost page # in right page at end of left apge
                left_page._internal_page.key_and_page[left_num_keys].key = middle_key;
                left_page._internal_page.key_and_page[left_num_keys].page_number =
                right_page._internal_page.leftmost_page_number;

                //set new middle key as right page's first key
                new_middle_key = right_page._internal_page.key_and_page[0].key;
                
                //shift left first first page # into leftmost page # (special case)
                right_page._internal_page.leftmost_page_number = right_page._internal_page.key_and_page[0].page_number;
                
                //shift left pair to fill space
                for(uint32_t i=0; i < right_num_keys - 1; i++){
                    right_page._internal_page.key_and_page[i] = right_page._internal_page.key_and_page[i + 1];
                }

                //wipe free space in right page
                right_page._internal_page.key_and_page[right_num_keys - 1] = {0,0};
                
                //update number of keys
                right_page._internal_page.page_header.number_of_keys --;
                left_page._internal_page.page_header.number_of_keys ++;

                //update parent page number in page moved to neighbor
                buffer_read_page(table_id, left_page._internal_page.key_and_page[left_num_keys].page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
                buffer_write_page(table_id, left_page._internal_page.key_and_page[left_num_keys].page_number, &tmp_page._raw_page);
            }
            else{
                //right page need one more key case

                //shift right pair to make space
                for(uint32_t i = right_num_keys; i>0; i--){
                    right_page._internal_page.key_and_page[i] = right_page._internal_page.key_and_page[i - 1];
                }

                //append middle key and leftmost page # in right page at first pair in right page
                right_page._internal_page.key_and_page[0].key = middle_key;
                right_page._internal_page.key_and_page[0].page_number = right_page._internal_page.leftmost_page_number;
                
                //set new middle key as left page's first key
                new_middle_key = left_page._internal_page.key_and_page[left_num_keys - 1].key;
                
                //moe last page # in left page into leftmost page # in right page
                right_page._internal_page.leftmost_page_number = 
                left_page._internal_page.key_and_page[left_num_keys - 1].page_number;
                
                //wipe free space in left page
                left_page._internal_page.key_and_page[left_num_keys - 1] = {0,0};
                
                //update number of keys
                left_page._internal_page.page_header.number_of_keys --;
                right_page._internal_page.page_header.number_of_keys ++;

                //update parent page number in page moved to neighbor
                buffer_read_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = right_page_number;
                buffer_write_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);
            }
        }

        buffer_read_page(table_id, left_page._internal_page.page_header.parent_page_number, &parent_page._raw_page); //get parent page
        uint32_t parent_num_keys = parent_page._internal_page.page_header.number_of_keys;

        //find old middle key's index and update key value with new middle key
        for(uint32_t i=0; i<parent_num_keys; i++){
            //find index with right page number
            if(parent_page._internal_page.key_and_page[i].page_number == right_page_number){
                parent_page._internal_page.key_and_page[i].key = new_middle_key;
                break;
            }
        }
        
        //save changes in three pages
        buffer_write_page(table_id, left_page._internal_page.page_header.parent_page_number, &parent_page._raw_page);
        buffer_write_page(table_id, right_page_number, &right_page._raw_page);
        buffer_write_page(table_id, left_page_number, &left_page._raw_page);

        return;
    }
}

int idx_insert_by_key(int64_t table_id, int64_t key, char *value, uint16_t val_size){
    try{
        return FIM::insert_record(table_id,key,value,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int idx_find_by_key(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size){
    try{
        return FIM::find_record(table_id,key,ret_val,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int idx_delete_by_key(int64_t table_id, int64_t key){
    try{
        return FIM::delete_record(table_id,key);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}