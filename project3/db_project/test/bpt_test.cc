#include <gtest/gtest.h>
#include "file.h"
#include "api.h"
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <random>

TEST(FileandIndexManager, BASIC_TEST){
    //init test
    
    srand(time(NULL)+1);
    init_db();
    char* path =  new char[26];
    const char *prefix = "./BS_";
    int len = strlen(prefix);
    memcpy(path, prefix ,len);
    for(int i=len;i<22;i++) path[i] = rand() % 26 + 'A';
    path[22] = '.';
    path[23] = 'd';
    path[24] = 'b';
    path[25] = 0;
    
    int64_t tid = open_table(path);

    int num = 40000;
    std::vector<int64_t> key_list;
    std::vector<char*> value_list;
    std::vector<int> siz_list;
    std::vector<int> idx_list;

    for(int64_t i=0; i<num; i++){
        key_list.push_back(i);
        int siz = MIN_VALUE_SIZE + rand() % (MAX_VALUE_SIZE - MIN_VALUE_SIZE);
        char* val = new char[siz];
        for(int j=0; j<siz-1; j++) val[j] = rand() % 26 + 'A';
        val[siz-1] = 0;
        value_list.push_back(val);
        siz_list.push_back(siz);
        idx_list.push_back(i);
    }

    std::random_device rd;
	std::mt19937 gen(rd());
    std::default_random_engine rng(rd());
    std::shuffle(idx_list.begin(), idx_list.end(), rng);

    for(int i=0;i<num;i++){
        EXPECT_EQ(db_insert(tid, key_list[idx_list[i]], value_list[idx_list[i]], siz_list[idx_list[i]]), 0);
        ASSERT_EQ(db_find(tid,key_list[idx_list[i]],NULL,NULL), 0)
        <<"CAN'T FIND "<<key_list[idx_list[i]]<<" in INSERTION\n";
    }
    shutdown_db();
    init_db();
    tid = open_table(path);
    
    std::shuffle(idx_list.begin(), idx_list.end(), rng);
    for(int j=0; j<num; j++){
        int i = idx_list[j];
        char * val = new char[128];
        uint16_t siz = 0;
        ASSERT_EQ(db_find(tid, key_list[i], val, &siz), 0)
        <<"CAN'T FIND "<<key_list[i]<<'\n';
        EXPECT_EQ(siz, siz_list[i]);
        EXPECT_EQ(strncmp(val, value_list[i], siz), 0);
        delete[] val;
    }

    EXPECT_NE(db_find(tid, num, NULL, NULL),0);
    EXPECT_NE(db_find(tid, -1, NULL, NULL),0);

    std::shuffle(idx_list.begin(), idx_list.end(), rng);
    for(int j=0; j<num; j++){
        int i = idx_list[j];
        ASSERT_EQ(db_delete(tid, key_list[i]), 0)
        <<j+1<<"th test: CAN'T DELETE "<<key_list[i]<<'\n';
        EXPECT_NE(db_find(tid, key_list[i], NULL, NULL),0);
    }


    //end test
    for(int i=0; i<num; i++) delete[] value_list[i];
    key_list.clear();
    value_list.clear();
    siz_list.clear();
    shutdown_db();
    remove(path);
    delete[] path;
}

TEST(FileandIndexManager, RANDOM_TEST){
    const int num = 5000; //number of record
    const int query = 100; //number of query
    bool random_seed = true; //set random_seed
    const int static_seed = 1234; //default static seed
    bool print_hash = true; //set printing hash
    bool print_key_and_value = true; //print final content
    bool delete_result_db = true; //delete db file option
    bool random_file_name = true; //set db file name random

    int prob_table[] = {85, 75, 95}; //set ratio of style of each type query

    //init test
    srand(random_seed?time(NULL):static_seed);
    init_db();

    char* path;
    if(random_file_name){
        //make random file name
        path =  new char[26];
        const char *prefix = "./RS_";
        int len = strlen(prefix);
        memcpy(path, prefix ,len);
        auto tm = time(NULL) % RAND_MAX;
        for(int i=len;i<22;i++){
            path[i] = tm % 26 + 'A';
            tm = (tm*tm)%RAND_MAX; 
        }
        path[22] = '.';
        path[23] = 'd';
        path[24] = 'b';
        path[25] = 0;
    }
    else{
        path = (char*)"STATIC_TEST_DB.db";
    }
    
    int64_t tid = open_table(path);

    //store record
    std::vector<int64_t> key_list;
    std::vector<char*> value_list;
    std::vector<int> siz_list;

    std::string result_string; //store find query

    for(int64_t i=0; i<num; i++){
        //make record
        key_list.push_back(i);

        int siz = MIN_VALUE_SIZE + rand() % (MAX_VALUE_SIZE - MIN_VALUE_SIZE);
        siz_list.push_back(siz);

        char* val = new char[siz];
        for(int j=0; j<siz-1; j++) val[j] = rand() % 26 + 'A';
        val[siz-1] = 0;
        value_list.push_back(val);
    }

    std::random_device rd;
	std::mt19937 gen(random_seed?rd():static_seed);
    std::default_random_engine rng(random_seed?rd():static_seed);
    shuffle(key_list.begin(),key_list.end(),rng);
    
    //in-memory record list
    std::vector<std::pair<int64_t, std::string>> key_value_pairs {};
    std::set<int64_t> key_set;

    int ratio = 2;

    for(int i=0;i<query;i++){
        int c = rand() % 4;
        int p = rand() % 100;
        bool is_empty = key_value_pairs.empty();
        std::uniform_int_distribution<int> list_dis(0, key_value_pairs.size() - 1);
        std::uniform_int_distribution<int> pool_dis(0, num - 1);

        if(c>=3){
            //find
            if((!is_empty && p<prob_table[0]) || key_value_pairs.size() >= num){
                //find existed case
                int idx = list_dis(gen);
                
                char * val = new char[128];
                uint16_t siz = 0;

                ASSERT_EQ(db_find(tid, key_value_pairs[idx].first, val, &siz), 0)
                <<"CAN'T FIND "<<key_value_pairs[idx].first<<'\n';

                EXPECT_EQ(siz, key_value_pairs[idx].second.size());
                ASSERT_EQ(strncmp(val, key_value_pairs[idx].second.c_str(), siz), 0)
                <<"NOT EQUAL "<<val<<' '<<key_value_pairs[idx].second<<" on query "<<i<<'\n';

                result_string += val;

                delete[] val;
            }
            else{
                //find nonexisted case
                int idx = 0;
                do{
                    idx = pool_dis(gen);
                }while(key_set.find(key_list[idx])!=key_set.end());

                ASSERT_NE(db_find(tid, key_list[idx], NULL, NULL), 0)
                <<"FIND NONEXISTED KEY "<<key_list[idx]<<'\n';
            }
        }
        else if(c<ratio){
            //insert
            if(is_empty || (p<prob_table[1] && key_value_pairs.size() < num)){
                //insert nonexisted key
                
                int idx = 0;
                do{
                    idx = pool_dis(gen);
                }while(key_set.find(key_list[idx])!=key_set.end());
                
                EXPECT_EQ(db_insert(tid, key_list[idx], value_list[idx], siz_list[idx]), 0);
                ASSERT_EQ(db_find(tid,key_list[idx],NULL,NULL), 0)
                <<"CAN'T FIND "<<key_list[idx]<<" in INSERTION\n";

                //apply changes
                key_set.insert(key_list[idx]);
                key_value_pairs.push_back({key_list[idx], std::string(value_list[idx], siz_list[idx])});
            }
            else{
                //insert existed key
                int idx = list_dis(gen);

                char * val = new char[128];

                ASSERT_NE(db_insert(tid, key_value_pairs[idx].first, val, key_value_pairs[idx].second.size()), 0)
                <<"INSERT EXISTED KEY "<<key_list[idx]<<'\n';

                delete[] val;
            }
            if(key_value_pairs.size() == num){
                //std::cerr << "full!"<<'\n';
                ratio = 1; //toggle raito
            }
        }
        else{
            //delete
            if((!is_empty && p<prob_table[2]) || key_value_pairs.size() >= num){
                //delete existed key
               int idx = list_dis(gen);
                
                ASSERT_EQ(db_delete(tid, key_value_pairs[idx].first), 0);
                ASSERT_NE(db_find(tid,key_value_pairs[idx].first,NULL,NULL), 0)
                <<"FIND DELETED "<<key_value_pairs[idx].first<<"in DELETION\n";

                //apply changes
                auto tmp = key_value_pairs[idx];
                key_set.erase(tmp.first);
                key_value_pairs.erase(std::find(key_value_pairs.begin(),key_value_pairs.end(),tmp));
            }
            else{
                //delete nonexisted key
                int idx = 0;
                do{
                    idx = pool_dis(gen);
                }while(key_set.find(key_list[idx])!=key_set.end());

                ASSERT_NE(db_delete(tid, key_list[idx]), 0)
                <<"DELETE NONEXISTED KEY "<<key_list[idx]<<'\n';
            }
            if(key_value_pairs.empty()){
                //std::cerr << "empty!"<<'\n';
                ratio = 2; //toggle raito
            }
        }
    }

    if(print_hash){
        size_t ret = std::hash<std::string>{}(result_string);
        std::cout<<"hash value: ";
        for(int i=0;i<16;i++){
            std::cout<<"0123456789ABCDEF"[ret&15];
            ret >>= 4;
        }
        std::cout<<'\n';
        std::cout<<"key len: "<<key_value_pairs.size()<<'\n';
    }
    if(print_key_and_value){
        int idx = 1;
        std::string pair_string = "";
        for(auto& x : key_value_pairs){
            std::cout<<idx++<<"th data -  "<<x.first<<" : "<<x.second<<'\n';
            pair_string += x.first + " " + x.second;
        }
        size_t ret = std::hash<std::string>{}(pair_string);
        std::cout<<"pair hash value: ";
        for(int i=0;i<16;i++){
            std::cout<<"0123456789ABCDEF"[ret&15];
            ret >>= 4;
        }
        std::cout<<'\n';
    }

    //end test
    for(int i=0; i<num; i++) delete[] value_list[i];
    key_list.clear();
    value_list.clear();
    siz_list.clear();
    shutdown_db();
    if(delete_result_db) remove(path);
    if(random_file_name) delete[] path;
}

TEST(FileandIndexManager, READ_TEST){
    bool print_hash = true; //set printing hash
    bool print_key_and_value = true; //print final content
    int64_t range = 200000; //key value range [0, range)
    char* path = (char*)"STATIC_TEST_DB.db";
    
    init_db();

    if(access(path,F_OK)){
        std::cout<<"NO DB FILE / SKIP READ TEST"<<'\n';
    }
    else{
        int64_t tid = open_table(path);
        std::string result_string; //store find query
        for(int64_t idx=0;idx<range;idx++){
            char * val = new char[128];
            uint16_t siz = 0;
            int ret = db_find(tid, idx, val, &siz);
            if(ret){
                val[0] = '0'; val[1] = 0;
            }
            if(idx % (range/20 + 1) == 0) std::cout<<idx<<"th data -  "<<idx<<" : "<<(ret?"NO DATA":val)<<'\n';
            result_string += val;
            delete[] val;
        }
        if(print_hash){
            size_t ret = std::hash<std::string>{}(result_string);
            std::cout<<"hash value: ";
            for(int i=0;i<16;i++){
                std::cout<<"0123456789ABCDEF"[ret&15];
                ret >>= 4;
            }
            std::cout<<'\n';
        }
    }
    //end test
    shutdown_db();
}