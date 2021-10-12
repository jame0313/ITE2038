#include <gtest/gtest.h>
#include "bpt.h"
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <random>

TEST(FileandIndexManager, INSERT){
    //init test
    srand(time(NULL));
    init_db();
    char* path =  new char[26];
    memcpy(path, "./DBEINSERT_",12);
    for(int i=12;i<22;i++) path[i] = rand() % 26 + 'A';
    path[22] = '.';
    path[23] = 'd';
    path[24] = 'b';
    path[25] = 0;
    
    int64_t tid = open_table(path);

    int num = 100000;
    std::vector<int64_t> key_list;
    std::vector<char*> value_list;
    std::vector<int> siz_list;
    std::vector<int> idx_list;

    for(int64_t i=0; i<num; i++){
        key_list.push_back(i*i);
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
        <<"CAN'T FIND "<<key_list[idx_list[i]]<<"in INSERTION\n";
    }
    shutdown_db();
    init_db();
    tid = open_table(path);
    for(int j=0; j<num; j++){
        int i = j;
        char * val = new char[128];
        uint16_t siz = 0;
        ASSERT_EQ(db_find(tid, key_list[i], val, &siz), 0)
        <<"CAN'T FIND "<<key_list[i]<<'\n';
        EXPECT_EQ(siz, siz_list[i]);
        EXPECT_EQ(strncmp(val, value_list[i], siz), 0);
        delete[] val;
    }

    EXPECT_NE(db_find(tid, 6, NULL, NULL),0);


    //end test
    for(int i=0; i<num; i++) delete[] value_list[i];
    key_list.clear();
    value_list.clear();
    siz_list.clear();
    shutdown_db();
    remove(path);
    delete[] path;
}