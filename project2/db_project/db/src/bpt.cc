#include "file.h"
#include "bpt.h"

namespace FIM{
    pagenum_t make_page(int64_t table_id){
        pagenum_t x = file_alloc_page(table_id);
        return x;
    }

    pagenum_t find_leaf_page(int64_t table_id, int64_t key){
        _fim_page_t header_page, cnt_page;
        file_read_page(table_id,0,&header_page._raw_page);
        pagenum_t root = header_page._header_page.root_page_number;
        if(!root) return 0;
        pagenum_t cnt_page_number = root;
        file_read_page(table_id,root,&cnt_page._raw_page);
        while(!cnt_page._leaf_page.page_header.is_leaf){
            uint32_t num_keys = cnt_page._internal_page.page_header.number_of_keys;
            if(key < cnt_page._internal_page.key_and_page[0].key){
                cnt_page_number = cnt_page._internal_page.leftmost_page_number;
            }
            else{
                for(uint32_t i = 0; i < num_keys; i++){
                    if(key < cnt_page._internal_page.key_and_page[i].key){
                        cnt_page_number = cnt_page._internal_page.key_and_page[i-1].page_number;
                        break;
                    }
                    else if(i+1 == num_keys){
                        cnt_page_number = cnt_page._internal_page.key_and_page[i].page_number;
                    }
                }
            }
            file_read_page(table_id,cnt_page_number,&cnt_page._raw_page);
        }
        return cnt_page_number;
    }

    int find_record(int64_t table_id, int64_t key, char *ret_val, uint16_t* val_size){
        pagenum_t leaf_page_number = FIM::find_leaf_page(table_id,key);
        if(!leaf_page_number) return -1;
        _fim_page_t leaf_page;
        file_read_page(table_id,leaf_page_number,&leaf_page._raw_page);
        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;
        for(uint32_t i = 0; i < num_keys; i++){
            if(leaf_page._leaf_page.slot[i].key == key){
                if(ret_val){
                    *val_size = leaf_page._leaf_page.slot[i].size;
                    memcpy(ret_val,leaf_page._raw_page.raw_data+(leaf_page._leaf_page.slot[i].offset),*val_size);
                }
                return 0;
            }
        }
        return -1;
    }

    int insert_record(int64_t table_id, int64_t key, char *value, uint16_t val_size){
        
        if(!FIM::find_record(table_id,key)) return -1;

        _fim_page_t header_page, leaf_page;
        file_read_page(table_id,0,&header_page._raw_page);
        pagenum_t root = header_page._header_page.root_page_number;

        if(!root){
            root = FIM::init_new_tree(table_id, key, value, val_size);
            file_read_page(table_id,0,&header_page._raw_page);
            header_page._header_page.root_page_number = root;
            file_write_page(table_id,0,&header_page._raw_page);
            return 0;
        }

        pagenum_t leaf_page_number = FIM::find_leaf_page(table_id,key);
        file_read_page(table_id,leaf_page_number,&leaf_page._raw_page);

        uint64_t left_space = leaf_page._leaf_page.amount_of_free_space;

        if(val_size + sizeof(FIM::page_slot_t) <= left_space){
            FIM::insert_into_leaf_page(leaf_page_number, table_id, key, value, val_size);
            return 0;
        }
        else{
            root = FIM::insert_into_leaf_page_after_splitting(leaf_page_number, table_id, key, value, val_size);
            if(root){
                file_read_page(table_id,0,&header_page._raw_page);
                header_page._header_page.root_page_number = root;
                file_write_page(table_id,0,&header_page._raw_page);
            }
            return 0;
        }
    }

    pagenum_t init_new_tree(int64_t table_id, int64_t key, char *value, uint16_t val_size){
        pagenum_t root = FIM::make_page(table_id);
        _fim_page_t root_page = {0,};
        //file_read_page(table_id,root,&root_page._raw_page);
        
        root_page._leaf_page.page_header.parent_page_number = 0;
        root_page._leaf_page.page_header.is_leaf = 1;
        root_page._leaf_page.page_header.number_of_keys = 1;
        root_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        root_page._leaf_page.amount_of_free_space -= val_size + sizeof(FIM::page_slot_t);
        root_page._leaf_page.right_sibling_page_number = 0;
        
        root_page._leaf_page.slot[0].key = key;
        root_page._leaf_page.slot[0].offset = PAGE_SIZE - val_size;
        root_page._leaf_page.slot[0].size = val_size;
        memcpy(root_page._raw_page.raw_data + (PAGE_SIZE - val_size), value, val_size);
        
        file_write_page(table_id,root,&root_page._raw_page);
        return root;
    }

    void insert_into_leaf_page(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size){
        _fim_page_t leaf_page;
        file_read_page(table_id,leaf_page_number,&leaf_page._raw_page);
        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;

        uint16_t offset = PAGE_SIZE;
        for(uint32_t i = 0; i < num_keys; i++) offset = std::min(offset, leaf_page._leaf_page.slot[i].offset);
        offset -= val_size;
        
        for(uint32_t i = 0; i < num_keys; i++){
            if(key < leaf_page._leaf_page.slot[i].key){
                for(uint32_t j = num_keys; j > i; j--){
                    leaf_page._leaf_page.slot[j] = leaf_page._leaf_page.slot[j-1];
                }
                leaf_page._leaf_page.slot[i].key = key;
                leaf_page._leaf_page.slot[i].size = val_size;
                leaf_page._leaf_page.slot[i].offset = offset;
                break;
            }
            else if(i+1 == num_keys){
                leaf_page._leaf_page.slot[num_keys].key = key;
                leaf_page._leaf_page.slot[num_keys].size = val_size;
                leaf_page._leaf_page.slot[num_keys].offset = offset;
            }
        }

        memcpy(leaf_page._raw_page.raw_data + offset, value, val_size);
        leaf_page._leaf_page.amount_of_free_space -= val_size + sizeof(FIM::page_slot_t);
        leaf_page._leaf_page.page_header.number_of_keys ++;
        file_write_page(table_id,leaf_page_number,&leaf_page._raw_page);
        return;
    }

    pagenum_t insert_into_leaf_page_after_splitting(pagenum_t leaf_page_number, int64_t table_id, int64_t key, char *value, uint16_t val_size){
        pagenum_t new_leaf_page_number = FIM::make_page(table_id);
        _fim_page_t new_leaf_page = {0,}, leaf_page;
        
        new_leaf_page._leaf_page.page_header.is_leaf = 1;
        
        file_read_page(table_id,leaf_page_number,&leaf_page._raw_page);
        uint32_t num_keys = leaf_page._leaf_page.page_header.number_of_keys;
        
        page_slot_t *tmp_slot = new page_slot_t[num_keys+1];
        char** value_list = new char* [num_keys+1];

        for(uint32_t i = 0, j = 0; j < num_keys + 1; i++,j++){
            if(i == j && key < leaf_page._leaf_page.slot[i].key){
                tmp_slot[j].key = key;
                tmp_slot[j].size = val_size;
                value_list[j] = new char[val_size+1];
                memcpy(value_list[j],value,val_size);
                value_list[j][val_size] = 0;
                j++;
            }

            tmp_slot[j] = leaf_page._leaf_page.slot[i];
            value_list[j] = new char[tmp_slot[j].size+1];
            memcpy(value_list[j],leaf_page._raw_page.raw_data+(tmp_slot[j].offset),tmp_slot[j].size);
            value_list[j][tmp_slot[j].size] = 0;

            if(i+1 == num_keys && i==j){
                j++;
                tmp_slot[j].key = key;
                tmp_slot[j].size = val_size;
                value_list[j] = new char[val_size+1];
                memcpy(value_list[j],value,val_size);
                value_list[j][val_size] = 0;
            }
        }

        uint16_t cnt_size_sum = 0;
        uint16_t threshold = (PAGE_SIZE - PAGE_HEADER_SIZE) >> 1;
        int split_point = -1;

        for(uint32_t i = 0; i < num_keys + 1; i++){
            uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t);
            if(cnt_size_sum + cnt_size >= threshold){
                split_point = i;
                break;
            }
            cnt_size_sum += cnt_size;
        }

        leaf_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
        new_leaf_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;

        for(uint32_t i = 0; i < split_point; i++){
            tmp_slot[i].offset = (i>0?leaf_page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size;
            leaf_page._leaf_page.slot[i] = tmp_slot[i];
            memcpy(leaf_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            leaf_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t);
        }

        for(uint32_t i = split_point; i < num_keys + 1; i++){
            tmp_slot[i].offset = (i-split_point>0?new_leaf_page._leaf_page.slot[i-split_point-1].offset:PAGE_SIZE) - tmp_slot[i].size;
            new_leaf_page._leaf_page.slot[i - split_point] = tmp_slot[i];
            memcpy(new_leaf_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            new_leaf_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t);
        }

        delete[] tmp_slot;
        for(int i = 0; i < num_keys + 1; i++) delete[] value_list[i];
        delete[] value_list;

        new_leaf_page._leaf_page.page_header.parent_page_number =
        leaf_page._leaf_page.page_header.parent_page_number;

        leaf_page._leaf_page.page_header.number_of_keys = split_point;
        new_leaf_page._leaf_page.page_header.number_of_keys = num_keys + 1 - split_point;
        new_leaf_page._leaf_page.right_sibling_page_number =
        leaf_page._leaf_page.right_sibling_page_number;
        leaf_page._leaf_page.right_sibling_page_number = new_leaf_page_number;
        
        memset(leaf_page._raw_page.raw_data + PAGE_HEADER_SIZE + (leaf_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, leaf_page._leaf_page.amount_of_free_space);
        memset(new_leaf_page._raw_page.raw_data + PAGE_HEADER_SIZE + (new_leaf_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, new_leaf_page._leaf_page.amount_of_free_space);

        file_write_page(table_id,leaf_page_number,&leaf_page._raw_page);
        file_write_page(table_id,new_leaf_page_number,&new_leaf_page._raw_page);

        int64_t new_key = new_leaf_page._leaf_page.slot[0].key;

        return FIM::insert_into_parent_page(leaf_page_number, table_id, new_key, new_leaf_page_number);
    }

    pagenum_t insert_into_parent_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        _fim_page_t left_page, right_page, parent_page;
        
        file_read_page(table_id,left_page_number,&left_page._raw_page);
        file_read_page(table_id,right_page_number,&right_page._raw_page);

        pagenum_t parent_page_number = left_page._internal_page.page_header.parent_page_number;
        if(!parent_page_number){
            return FIM::insert_into_new_root_page(left_page_number, table_id, key, right_page_number);
        }

        file_read_page(table_id,parent_page_number,&parent_page._raw_page);

        uint32_t num_keys = parent_page._internal_page.page_header.number_of_keys;

        if(num_keys + 1 <= MAX_KEY_NUMBER){
            FIM::insert_into_page(parent_page_number, left_page_number, table_id, key, right_page_number);
            return 0;
        }
        else{
            return FIM::insert_into_page_after_splitting(parent_page_number, left_page_number, table_id, key, right_page_number);
        }
    }

    pagenum_t insert_into_new_root_page(pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        pagenum_t root = FIM::make_page(table_id);
        _fim_page_t root_page = {0,}, left_page, right_page;
        //file_read_page(table_id,root,&root_page._raw_page);
        file_read_page(table_id,left_page_number,&left_page._raw_page);
        file_read_page(table_id,right_page_number,&right_page._raw_page);
        
        root_page._internal_page.page_header.parent_page_number = 0;
        root_page._internal_page.page_header.number_of_keys = 1;

        root_page._internal_page.leftmost_page_number = left_page_number;
        root_page._internal_page.key_and_page[0].key = key;
        root_page._internal_page.key_and_page[0].page_number = right_page_number;

        left_page._internal_page.page_header.parent_page_number = root;
        right_page._internal_page.page_header.parent_page_number = root;

        file_write_page(table_id,root,&root_page._raw_page);
        file_write_page(table_id,left_page_number,&left_page._raw_page);
        file_write_page(table_id,right_page_number,&right_page._raw_page);

        return root;
    }
    
    void insert_into_page(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        _fim_page_t page;
        file_read_page(table_id,page_number,&page._raw_page);
        
        uint32_t num_keys = page._internal_page.page_header.number_of_keys;
        int left_index = -1;

        for(uint32_t i=0; i<num_keys; i++){
            if(page._internal_page.key_and_page[i].page_number == left_page_number){
                left_index = i;
                break;
            }
        }

        for(uint32_t i = num_keys; i>left_index + 1; i--){
            page._internal_page.key_and_page[i] = page._internal_page.key_and_page[i-1];
        }

        page._internal_page.key_and_page[left_index + 1].key = key;
        page._internal_page.key_and_page[left_index + 1].page_number = right_page_number;
        
        page._internal_page.page_header.number_of_keys ++;

        file_write_page(table_id,page_number,&page._raw_page);
        return;
    }

    pagenum_t insert_into_page_after_splitting(pagenum_t page_number, pagenum_t left_page_number, int64_t table_id, int64_t key, pagenum_t right_page_number){
        pagenum_t new_page_number = FIM::make_page(table_id);
        _fim_page_t new_page = {0,}, page, tmp_page;
        
        file_read_page(table_id,page_number,&page._raw_page);
        uint32_t num_keys = page._internal_page.page_header.number_of_keys;
        
        pagenum_t tmp_leftmost_page_number = page._internal_page.leftmost_page_number;
        keypagenum_pair_t *tmp_pair = new keypagenum_pair_t[num_keys+1];


        for(uint32_t i = 0, j = 0; j < num_keys + 1; i++,j++){
            if(i == 0 && tmp_leftmost_page_number == left_page_number){
                tmp_pair[j].key = key;
                tmp_pair[j].page_number = right_page_number;
                j++;
            }

            tmp_pair[j] = page._internal_page.key_and_page[i];

            if(page._internal_page.key_and_page[i].page_number == left_page_number){
                j++;
                tmp_pair[j].key = key;
                tmp_pair[j].page_number = right_page_number;
            }
        }

        int split_point = DEFAULT_ORDER;
        
        for(uint32_t i = 0; i < split_point; i++){
            page._internal_page.key_and_page[i] = tmp_pair[i];
        }

        int64_t new_key = tmp_pair[split_point].key;
        new_page._internal_page.leftmost_page_number = tmp_pair[split_point].page_number;
        
        for(uint32_t i = split_point + 1; i < num_keys + 1; i++){
            new_page._internal_page.key_and_page[i - split_point - 1] = tmp_pair[i];
        }

        delete[] tmp_pair;

        new_page._internal_page.page_header.parent_page_number =
        page._internal_page.page_header.parent_page_number;

        page._internal_page.page_header.number_of_keys = split_point;
        new_page._internal_page.page_header.number_of_keys = split_point;

        int64_t offset = PAGE_HEADER_SIZE + split_point*(sizeof(FIM::keypagenum_pair_t));

        memset(page._raw_page.raw_data + offset, 0, PAGE_SIZE - offset);
        memset(new_page._raw_page.raw_data + offset, 0, PAGE_SIZE - offset);
        
        file_write_page(table_id,page_number,&page._raw_page);
        file_write_page(table_id,new_page_number,&new_page._raw_page);

        file_read_page(table_id,new_page._internal_page.leftmost_page_number,&tmp_page._raw_page);
        tmp_page._internal_page.page_header.parent_page_number = new_page_number;
        file_write_page(table_id,new_page._internal_page.leftmost_page_number,&tmp_page._raw_page);
        
        for(uint32_t i = 0; i < split_point; i++){
            file_read_page(table_id,new_page._internal_page.key_and_page[i].page_number,&tmp_page._raw_page);
            tmp_page._internal_page.page_header.parent_page_number = new_page_number;
            file_write_page(table_id,new_page._internal_page.key_and_page[i].page_number,&tmp_page._raw_page);
        }

        return FIM::insert_into_parent_page(page_number, table_id, new_key, new_page_number);
    }

    int delete_record(int64_t table_id, int64_t key){

        if(FIM::find_record(table_id,key) != 0) return -1;

        pagenum_t leaf_page = FIM::find_leaf_page(table_id, key);

        return FIM::delete_entry(leaf_page, table_id, key);
    }

    int delete_entry(pagenum_t page_number, int64_t table_id, int64_t key){
        _fim_page_t page, header_page, parent_page, neighbor_page;
        
        FIM::remove_entry_from_page(page_number, table_id, key);

        file_read_page(table_id,page_number,&page._raw_page);

        pagenum_t parent_page_number = page._internal_page.page_header.parent_page_number;
        
        if(!parent_page_number){
            pagenum_t root = FIM::adjust_root_page(page_number, table_id);
            file_read_page(table_id,0,&header_page._raw_page);
            header_page._header_page.root_page_number = root;
            file_write_page(table_id,0,&header_page._raw_page);
            return 0;
        }

        bool is_modify_needed;
        uint64_t maximum_free_space = MAX_FREE_SPACE;
        uint32_t min_keys = DEFAULT_ORDER;
        
        uint32_t is_leaf = page._leaf_page.page_header.is_leaf;
        if(is_leaf){
            is_modify_needed = page._leaf_page.amount_of_free_space >= maximum_free_space;
        }
        else{
            is_modify_needed = page._internal_page.page_header.number_of_keys < DEFAULT_ORDER;
        }

        if(is_modify_needed){
            file_read_page(table_id,parent_page_number,&parent_page._raw_page);
            pagenum_t neighbor_page_number;
            int64_t middle_key;
            bool is_leftmost = false;
            if(parent_page._internal_page.leftmost_page_number == page_number){
                is_leftmost = true;
                middle_key = parent_page._internal_page.key_and_page[0].key;
                neighbor_page_number = parent_page._internal_page.key_and_page[0].page_number;
            }
            else{
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

            file_read_page(table_id,neighbor_page_number,&neighbor_page._raw_page);
            bool can_merge;

            if(is_leaf){
                can_merge = page._leaf_page.amount_of_free_space + neighbor_page._leaf_page.amount_of_free_space >= (PAGE_SIZE - PAGE_HEADER_SIZE);
            }
            else{
                can_merge = page._internal_page.page_header.number_of_keys + neighbor_page._internal_page.page_header.number_of_keys + 1 <= MAX_KEY_NUMBER;
            }

            if(can_merge){
                return FIM::merge_pages(page_number, table_id, middle_key, neighbor_page_number, is_leftmost);
            }
            else{
                FIM::redistribute_pages(page_number, table_id, middle_key, neighbor_page_number, is_leftmost);
                return 0;
            }
        }
        else return 0;
    }

    void remove_entry_from_page(pagenum_t page_number, uint64_t table_id, int64_t key){
        _fim_page_t page;
        file_read_page(table_id, page_number, &page._raw_page);
        uint32_t num_keys = page._leaf_page.page_header.number_of_keys;
        if(page._leaf_page.page_header.is_leaf){
            page_slot_t *tmp_slot = new page_slot_t[num_keys-1];
            char **value_list = new char* [num_keys-1];
            
            for(uint32_t i=0, j=0; i<num_keys; i++, j++){
                if(page._leaf_page.slot[i].key == key){
                    page._leaf_page.amount_of_free_space += page._leaf_page.slot[i].size + sizeof(FIM::page_slot_t);
                    i++;
                    if(i>=num_keys) break;
                }
                tmp_slot[j] = page._leaf_page.slot[i];
                value_list[j] = new char [tmp_slot[j].size+1];
                memcpy(value_list[j], page._raw_page.raw_data + (tmp_slot[j].offset), tmp_slot[j].size);
                value_list[j][tmp_slot[j].size] = 0;
            }
            for(uint32_t i=0; i<num_keys-1 ;i++){
                tmp_slot[i].offset = (i>0?page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size;
                page._leaf_page.slot[i] = tmp_slot[i];
                memcpy(page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
            }

            delete[] tmp_slot;
            for(int i = 0; i < num_keys - 1; i++) delete[] value_list[i];
            delete[] value_list;

            memset(page._raw_page.raw_data + PAGE_HEADER_SIZE + ((num_keys-1)*sizeof(FIM::page_slot_t)), 0, page._leaf_page.amount_of_free_space);

        }
        else{
            for(uint32_t i=0; i<num_keys; i++){
                if(page._internal_page.key_and_page[i].key == key){
                    for(uint32_t j=i+1; j<num_keys; j++){
                        page._internal_page.key_and_page[j-1] = page._internal_page.key_and_page[j];
                    }
                    page._internal_page.key_and_page[num_keys - 1] = {0,0};
                    break;
                }
                
            }
        }
        page._leaf_page.page_header.number_of_keys --;
        file_write_page(table_id, page_number, &page._raw_page);
    }

    pagenum_t adjust_root_page(pagenum_t root_page_number, int64_t table_id){
        _fim_page_t root_page, new_root_page;
        file_read_page(table_id, root_page_number, &root_page._raw_page);
        
        uint32_t num_keys = root_page._leaf_page.page_header.number_of_keys;
        pagenum_t new_root_page_number;
        
        if(num_keys > 0) return root_page_number;

        if(root_page._leaf_page.page_header.is_leaf){
            new_root_page_number =  0;
        }
        else{
            new_root_page_number = root_page._internal_page.leftmost_page_number;
            file_read_page(table_id, new_root_page_number, &new_root_page._raw_page);
            new_root_page._leaf_page.page_header.parent_page_number = 0;
            file_write_page(table_id, new_root_page_number, &new_root_page._raw_page);
        }
        file_free_page(table_id, root_page_number);

        return new_root_page_number;
    }

    int merge_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost){
        _fim_page_t left_page, right_page, tmp_page;
        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        pagenum_t right_page_number = page_number;
        pagenum_t left_page_number = neighbor_page_number;
        file_read_page(table_id, page_number, &right_page._raw_page);
        file_read_page(table_id, neighbor_page_number, &left_page._raw_page);

        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        uint32_t left_num_keys = left_page._leaf_page.page_header.number_of_keys;
        uint32_t right_num_keys = right_page._leaf_page.page_header.number_of_keys;

        if(left_page._leaf_page.page_header.is_leaf){
            uint16_t offset = PAGE_SIZE;
            for(uint32_t i = 0; i < left_num_keys; i++) offset = std::min(offset, left_page._leaf_page.slot[i].offset);
            

            for(uint32_t i=0; i<right_num_keys; i++){
                left_page._leaf_page.slot[left_num_keys + i] = right_page._leaf_page.slot[i];
                left_page._leaf_page.slot[left_num_keys + i].offset = (i > 0?
                    left_page._leaf_page.slot[left_num_keys + i - 1].offset:
                    offset)
                    - right_page._leaf_page.slot[i].size;
                memcpy(left_page._raw_page.raw_data + left_page._leaf_page.slot[left_num_keys + i].offset,
                    right_page._raw_page.raw_data + right_page._leaf_page.slot[i].offset,
                    left_page._leaf_page.slot[left_num_keys + i].size);
                left_page._leaf_page.amount_of_free_space -= left_page._leaf_page.slot[left_num_keys + i].size + sizeof(FIM::page_slot_t);
            }
            left_page._leaf_page.page_header.number_of_keys += right_num_keys;
            left_page._leaf_page.right_sibling_page_number = right_page._leaf_page.right_sibling_page_number;
        }
        else{
            for(uint32_t i=0; i<right_num_keys; i++){
                file_read_page(table_id, right_page._internal_page.key_and_page[i].page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
                file_write_page(table_id, right_page._internal_page.key_and_page[i].page_number, &tmp_page._raw_page);
            }

            file_read_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);
            tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
            file_write_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);

            left_page._internal_page.key_and_page[left_num_keys].key = middle_key;
            left_page._internal_page.key_and_page[left_num_keys].page_number =
            right_page._internal_page.leftmost_page_number;

            for(uint32_t i=0; i<right_num_keys; i++){
                left_page._internal_page.key_and_page[left_num_keys + 1 + i] =
                right_page._internal_page.key_and_page[i];
            }
            left_page._leaf_page.page_header.number_of_keys += right_num_keys + 1;
        }

        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        file_write_page(table_id, neighbor_page_number, &left_page._raw_page);

        pagenum_t parent_page_number = left_page._leaf_page.page_header.parent_page_number;
        file_free_page(table_id, page_number);
        return FIM::delete_entry(parent_page_number, table_id, middle_key);
    }

    void redistribute_pages(pagenum_t page_number, int64_t table_id, int64_t middle_key, pagenum_t neighbor_page_number, bool is_leftmost){
        _fim_page_t left_page, right_page, tmp_page, parent_page;
        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        pagenum_t right_page_number = page_number;
        pagenum_t left_page_number = neighbor_page_number;
        file_read_page(table_id, page_number, &right_page._raw_page);
        file_read_page(table_id, neighbor_page_number, &left_page._raw_page);

        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        uint32_t left_num_keys = left_page._leaf_page.page_header.number_of_keys;
        uint32_t right_num_keys = right_page._leaf_page.page_header.number_of_keys;
        int64_t new_middle_key;

        if(left_page._leaf_page.page_header.is_leaf){
            uint64_t left_free_space = left_page._leaf_page.amount_of_free_space;
            uint64_t right_free_space = right_page._leaf_page.amount_of_free_space;
            
            page_slot_t *tmp_slot = new page_slot_t[left_num_keys+right_num_keys];
            char** value_list = new char* [left_num_keys+right_num_keys];

            for(uint32_t i=0; i<left_num_keys; i++){
                tmp_slot[i] = left_page._leaf_page.slot[i];
                value_list[i] = new char[tmp_slot[i].size+1];
                memcpy(value_list[i],left_page._raw_page.raw_data+(tmp_slot[i].offset),tmp_slot[i].size);
                value_list[i][tmp_slot[i].size] = 0;
            }
            for(uint32_t j=0, i=left_num_keys; j<right_num_keys; i++, j++){
                tmp_slot[i] = right_page._leaf_page.slot[j];
                value_list[i] = new char[tmp_slot[i].size+1];
                memcpy(value_list[i],right_page._raw_page.raw_data+(tmp_slot[i].offset),tmp_slot[i].size);
                value_list[i][tmp_slot[i].size] = 0;
            }

            uint16_t cnt_size_sum = 0;
            uint16_t threshold = MAX_FREE_SPACE;
            int split_point = -1;
            
            if(left_free_space>right_free_space){
               for(uint32_t i = left_num_keys; i < left_num_keys + right_num_keys; i++){
                    uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t);
                    if(left_free_space - (cnt_size_sum + cnt_size) < threshold){
                        split_point = i + 1;
                        break;
                    }
                    cnt_size_sum += cnt_size;
                }
            }
            else{
                for(uint32_t i = left_num_keys - 1; i > 0; i--){
                    uint32_t cnt_size = tmp_slot[i].size + sizeof(FIM::page_slot_t);
                    if(right_free_space - (cnt_size_sum + cnt_size) < threshold){
                        split_point = i;
                        break;
                    }
                    cnt_size_sum += cnt_size;
                }
            }

            left_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;
            right_page._leaf_page.amount_of_free_space = PAGE_SIZE - PAGE_HEADER_SIZE;

            for(uint32_t i = 0; i < split_point; i++){
                tmp_slot[i].offset = (i>0?left_page._leaf_page.slot[i-1].offset:PAGE_SIZE) - tmp_slot[i].size;
                left_page._leaf_page.slot[i] = tmp_slot[i];
                memcpy(left_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
                left_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t);
            }

            for(uint32_t i = split_point; i < left_num_keys + right_num_keys; i++){
                tmp_slot[i].offset = (i-split_point>0?right_page._leaf_page.slot[i-split_point-1].offset:PAGE_SIZE) - tmp_slot[i].size;
                right_page._leaf_page.slot[i - split_point] = tmp_slot[i];
                memcpy(right_page._raw_page.raw_data + tmp_slot[i].offset, value_list[i], tmp_slot[i].size);
                right_page._leaf_page.amount_of_free_space -= tmp_slot[i].size + sizeof(FIM::page_slot_t);
            }

            delete[] tmp_slot;
            for(int i = 0; i < left_num_keys+right_num_keys; i++) delete[] value_list[i];
            delete[] value_list;

            left_page._leaf_page.page_header.number_of_keys = split_point;
            right_page._leaf_page.page_header.number_of_keys = left_num_keys + right_num_keys - split_point;

            memset(left_page._raw_page.raw_data + PAGE_HEADER_SIZE + (left_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, left_page._leaf_page.amount_of_free_space);
            memset(right_page._raw_page.raw_data + PAGE_HEADER_SIZE + (right_page._leaf_page.page_header.number_of_keys*sizeof(FIM::page_slot_t)), 0, right_page._leaf_page.amount_of_free_space);
            
            new_middle_key = right_page._leaf_page.slot[0].key;

            
        }
        else{
            if(left_num_keys<right_num_keys){
                left_page._internal_page.key_and_page[left_num_keys].key = middle_key;
                left_page._internal_page.key_and_page[left_num_keys].page_number =
                right_page._internal_page.leftmost_page_number;

                new_middle_key = right_page._internal_page.key_and_page[0].key;
                right_page._internal_page.leftmost_page_number = right_page._internal_page.key_and_page[0].page_number;

                for(uint32_t i=0; i< right_num_keys - 1; i++){
                    right_page._internal_page.key_and_page[i] = right_page._internal_page.key_and_page[i + 1];
                }

                right_page._internal_page.key_and_page[right_num_keys - 1] = {0,0};
                right_page._internal_page.page_header.number_of_keys --;

                left_page._internal_page.page_header.number_of_keys ++;

                file_read_page(table_id, left_page._internal_page.key_and_page[left_num_keys].page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = left_page_number;
                file_write_page(table_id, left_page._internal_page.key_and_page[left_num_keys].page_number, &tmp_page._raw_page);
            }
            else{
                for(uint32_t i = right_num_keys; i>0; i--){
                    right_page._internal_page.key_and_page[i] = right_page._internal_page.key_and_page[i - 1];
                }
                right_page._internal_page.key_and_page[0].key = middle_key;
                right_page._internal_page.key_and_page[0].page_number = right_page._internal_page.leftmost_page_number;
                right_page._internal_page.leftmost_page_number = 
                left_page._internal_page.key_and_page[left_num_keys - 1].page_number;
                
                new_middle_key = left_page._internal_page.key_and_page[left_num_keys - 1].key;
                
                left_page._internal_page.key_and_page[left_num_keys - 1] = {0,0};
                left_page._internal_page.page_header.number_of_keys --;

                right_page._internal_page.page_header.number_of_keys ++;

                file_read_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);
                tmp_page._leaf_page.page_header.parent_page_number = right_page_number;
                file_write_page(table_id, right_page._internal_page.leftmost_page_number, &tmp_page._raw_page);

            }

            
            
        }

        file_read_page(table_id, left_page._internal_page.page_header.parent_page_number, &parent_page._raw_page);
        uint32_t parent_num_keys = parent_page._internal_page.page_header.number_of_keys;
        for(uint32_t i=0; i<parent_num_keys; i++){
            if(parent_page._internal_page.key_and_page[i].page_number == right_page_number){
                parent_page._internal_page.key_and_page[i].key = new_middle_key;
                break;
            }
        }
        file_write_page(table_id, left_page._internal_page.page_header.parent_page_number, &parent_page._raw_page);

        if(is_leftmost) std::swap(page_number, neighbor_page_number);

        file_write_page(table_id, page_number, &right_page._raw_page);
        file_write_page(table_id, neighbor_page_number, &left_page._raw_page);

        return;
    }


}


int64_t open_table(char *pathname){
    try{
        int64_t tid = file_open_table_file(pathname);
        return tid;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size){
    try{
        return FIM::insert_record(table_id,key,value,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size){
    try{
        return FIM::find_record(table_id,key,ret_val,val_size);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int db_delete(int64_t table_id, int64_t key){
    try{
        return FIM::delete_record(table_id,key);
    }
    catch(const char *e){
        perror(e);
        return -1;
    }
}

int init_db(){
    return 0;
}

int shutdown_db(){
    try{
        file_close_table_file();
        return 0;
    }catch(const char *e){
        perror(e);
        return -1;
    }
}