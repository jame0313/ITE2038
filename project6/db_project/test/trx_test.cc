#include <gtest/gtest.h>
#include "file.h"
#include "api.h"
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <random>
#include <pthread.h>

void* txn_func(void *arg){
    void **argv = (void**)arg;
    std::vector<int64_t> *key_list = reinterpret_cast<std::vector<int64_t> *>(argv[0]);
    std::vector<char*> *value_list = reinterpret_cast<std::vector<char*> *>(argv[1]);
    std::vector<int> *siz_list = reinterpret_cast<std::vector<int> *>(argv[2]);
    int num = *reinterpret_cast<int*>(argv[3]);
    int query = *reinterpret_cast<int*>(argv[4]);
    std::mt19937 *gen = reinterpret_cast<std::mt19937*>(argv[5]);
    int tid = *reinterpret_cast<int*>(argv[6]);
    int prob_table[] = {95, 95};
    int trx_id = trx_begin();
    
    EXPECT_NE(trx_id, 0)<<"CAN'T BEGIN TXN\n";
    if(trx_id==0){
        return NULL;
    }
    std::cout<<"TXN "<<trx_id<<" BEGIN\n";
    
    int prekey=1;
    std::uniform_int_distribution<int> list_dis(1, std::max(1, num/query));

    for(int i=0; i<query; i++){
        int c = rand() % 2;
        int p = rand() % 100;
        
        if(c==0){
            //find
            if(p<prob_table[0]){
                //find existed case
                int idx = prekey + list_dis(*gen);
                if(idx>=num) break;
                char * val = new char[128];
                uint16_t siz = 0;
                EXPECT_EQ(db_find(tid, (*key_list)[idx], val, &siz, trx_id), 0)
                <<"CAN'T FIND "<<(*key_list)[idx]<<'\n';
                //std::cout<<"TXN "<<trx_id<<' '<<"FIND "<<(*key_list)[idx]<<'\n';
                EXPECT_EQ(siz, (*siz_list)[idx]);
                EXPECT_EQ(strncmp(val, (*value_list)[idx], siz), 0);
                delete[] val;
                prekey = idx;
            }
            else{
                //find nonexisted case
                int idx = list_dis(*gen);
                idx = rand()%2?-idx-1:idx+num;
                EXPECT_NE(db_find(tid, idx, NULL, NULL, trx_id),0)
                <<"FIND NONEXISTED KEY "<<idx<<'\n';
            }
        }
        else if(c==1){
            //update
            if(p<prob_table[1]){
                //update existed case
                int idx = prekey + list_dis(*gen);
                if(idx>=num) break;
                char * val = new char[128];
                uint16_t siz = (*siz_list)[idx];
                for(int j=0; j<siz-1; j++) val[j] = rand() % 26 + 'A';
                EXPECT_EQ(db_update(tid, (*key_list)[idx], val, siz, &siz, trx_id), 0)
                <<"CAN'T UPDATE "<<(*key_list)[idx]<<'\n';
                //std::cout<<"TXN "<<trx_id<<' '<<"UPDATE "<<(*key_list)[idx]<<'\n';
                memcpy((*value_list)[idx],val,siz);
                EXPECT_EQ(db_find(tid, (*key_list)[idx], val, &siz, trx_id), 0)
                <<"CAN'T FIND "<<(*key_list)[idx]<<"AFTER UPDATE"<<'\n';
                EXPECT_EQ(siz, (*siz_list)[idx]);
                EXPECT_EQ(strncmp(val, (*value_list)[idx], siz), 0);
                delete[] val;
                prekey = idx;
            }
            else{
                //update nonexisted case
                int idx = list_dis(*gen);
                idx = rand()%2?-idx-1:idx+num;
                EXPECT_NE(db_update(tid, idx, NULL, 0, NULL, trx_id),0)
                <<"UPDATE NONEXISTED KEY "<<idx<<'\n';
            }
        }
    }
    trx_id = trx_commit(trx_id);
    EXPECT_NE(trx_id, 0)<<"CAN'T COMMIT TXN\n";
    if(trx_id){
        std::cout<<"TXN "<<trx_id<<" END\n";
    }
    return (void*)"OK";
}

TEST(TransactionManager, SINGLE_THREAD_TEST){
    const int num = 4000; //number of record
    const int query = 800000; //number of query
    bool random_seed = true; //set random_seed
    const int static_seed = 1234; //default static seed

    int prob_table[] = {95, 95}; //set ratio of style of each type query

    //init test
    srand(random_seed?time(NULL):static_seed);
    init_db();

    char* path =  new char[26];
    const char *prefix = "./ST_";
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
    
    int64_t tid = open_table(path);

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
	std::mt19937 gen(random_seed?rd():static_seed);
    std::default_random_engine rng(random_seed?rd():static_seed);
    std::shuffle(idx_list.begin(), idx_list.end(), rng);

    for(int i=0;i<num;i++){
        EXPECT_EQ(db_insert(tid, key_list[idx_list[i]], value_list[idx_list[i]], siz_list[idx_list[i]]), 0);
        ASSERT_EQ(db_find(tid,key_list[idx_list[i]],NULL,NULL), 0)
        <<"CAN'T FIND "<<key_list[idx_list[i]]<<" in INSERTION\n";
    }
    shutdown_db();
    init_db();
    tid = open_table(path);

    int trx_id = trx_begin();
    ASSERT_NE(trx_id, 0)<<"CAN'T BEGIN TXN\n";
    std::shuffle(idx_list.begin(), idx_list.end(), rng);
    std::cout<<"TXN BEGIN"<<'\n';
    for(int i=0; i<query; i++){
        int c = rand() % 2;
        int p = rand() % 100;
        std::uniform_int_distribution<int> list_dis(0, num - 1);

        if(c==0){
            //find
            if(p<prob_table[0]){
                //find existed case
                int idx = list_dis(gen);
                char * val = new char[128];
                uint16_t siz = 0;
                ASSERT_EQ(db_find(tid, key_list[idx], val, &siz, trx_id), 0)
                <<"CAN'T FIND "<<key_list[idx]<<'\n';
                EXPECT_EQ(siz, siz_list[idx]);
                EXPECT_EQ(strncmp(val, value_list[idx], siz), 0);
                delete[] val;
            }
            else{
                //find nonexisted case
                int idx = list_dis(gen);
                idx = rand()%2?-idx-1:idx+num;
                EXPECT_NE(db_find(tid, idx, NULL, NULL, trx_id),0)
                <<"FIND NONEXISTED KEY "<<idx<<'\n';
            }
        }
        else if(c==1){
            //update
            if(p<prob_table[1]){
                //update existed case
                int idx = list_dis(gen);
                char * val = value_list[idx];
                uint16_t siz = siz_list[idx];
                for(int j=0; j<siz-1; j++) val[j] = rand() % 26 + 'A';
                ASSERT_EQ(db_update(tid, key_list[idx], val, siz, &siz, trx_id), 0)
                <<"CAN'T UPDATE "<<key_list[idx]<<'\n';

                val = new char[128];
                ASSERT_EQ(db_find(tid, key_list[idx], val, &siz, trx_id), 0)
                <<"CAN'T FIND "<<key_list[idx]<<"AFTER UPDATE"<<'\n';
                EXPECT_EQ(siz, siz_list[idx]);
                EXPECT_EQ(strncmp(val, value_list[idx], siz), 0);
                delete[] val;
            }
            else{
                //update nonexisted case
                int idx = list_dis(gen);
                idx = rand()%2?-idx-1:idx+num;
                EXPECT_NE(db_update(tid, idx, NULL, 0, NULL, trx_id),0)
                <<"UPDATE NONEXISTED KEY "<<idx<<'\n';
            }
        }
    }

    ASSERT_EQ(trx_commit(trx_id),trx_id)<<"CAN'T COMMIT TXN\n";
    std::cout<<"TXN END"<<'\n';
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

TEST(TransactionManager, SIMPLE_SHARED_LOCK_TEST){
    const int num = 4000; //number of record
    const int query = 800000; //number of query
    bool random_seed = true; //set random_seed
    const int static_seed = 1234; //default static seed
    const int thread_number = 10;

    int prob_table[] = {95}; //set ratio of style of each type query

    int threads[thread_number + 1];

    //init test
    srand(random_seed?time(NULL):static_seed);
    init_db();

    char* path =  new char[26];
    const char *prefix = "./SSL_";
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
    
    int64_t tid = open_table(path);

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
	std::mt19937 gen(random_seed?rd():static_seed);
    std::default_random_engine rng(random_seed?rd():static_seed);
    std::shuffle(idx_list.begin(), idx_list.end(), rng);

    for(int i=0;i<num;i++){
        EXPECT_EQ(db_insert(tid, key_list[idx_list[i]], value_list[idx_list[i]], siz_list[idx_list[i]]), 0);
        ASSERT_EQ(db_find(tid,key_list[idx_list[i]],NULL,NULL), 0)
        <<"CAN'T FIND "<<key_list[idx_list[i]]<<" in INSERTION\n";
    }
    shutdown_db();
    init_db();
    tid = open_table(path);

    for(int i=1; i<=thread_number; i++){
        int trx_id = trx_begin();
        ASSERT_NE(trx_id, 0)<<"CAN'T BEGIN TXN\n";
    }

    for(int i=0; i<query; i++){
        int p = rand() % 100;
        int trx_id = (rand() % thread_number) + 1;
        std::uniform_int_distribution<int> list_dis(0, num - 1);

        //find
        if(p<prob_table[0]){
            //find existed case
            int idx = list_dis(gen);
            char * val = new char[128];
            uint16_t siz = 0;
            ASSERT_EQ(db_find(tid, key_list[idx], val, &siz, trx_id), 0)
            <<"CAN'T FIND "<<key_list[idx]<<'\n';
            EXPECT_EQ(siz, siz_list[idx]);
            EXPECT_EQ(strncmp(val, value_list[idx], siz), 0);
            delete[] val;
        }
        else{
            //find nonexisted case
            int idx = list_dis(gen);
            idx = rand()%2?-idx-1:idx+num;
            EXPECT_NE(db_find(tid, idx, NULL, NULL, trx_id),0)
            <<"FIND NONEXISTED KEY "<<idx<<'\n';
        }
        
    }

    for(int i=1; i<=thread_number; i++){
        ASSERT_EQ(trx_commit(i),i)<<"CAN'T COMMIT TXN\n";
    }

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

TEST(TransactionManager, RANDOM_MULTI_THREAD_TEST){
    const int num = 4000; //number of record
    const int query = 800000; //number of query
    bool random_seed = true; //set random_seed
    const int static_seed = 1234; //default static seed
    const int thread_number = 5;

    int prob_table[] = {95, 95}; //set ratio of style of each type query

    pthread_t threads[thread_number];

    //init test
    srand(random_seed?time(NULL):static_seed);
    init_db();

    char* path =  new char[26];
    const char *prefix = "./RMT_";
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
    
    int64_t tid = open_table(path);

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
	std::mt19937 gen(random_seed?rd():static_seed);
    std::default_random_engine rng(random_seed?rd():static_seed);
    std::shuffle(idx_list.begin(), idx_list.end(), rng);

    for(int i=0;i<num;i++){
        EXPECT_EQ(db_insert(tid, key_list[idx_list[i]], value_list[idx_list[i]], siz_list[idx_list[i]]), 0);
        ASSERT_EQ(db_find(tid,key_list[idx_list[i]],NULL,NULL), 0)
        <<"CAN'T FIND "<<key_list[idx_list[i]]<<" in INSERTION\n";
    }
    shutdown_db();
    init_db();
    tid = open_table(path);

    void* args[] = {
        (void*)(&key_list),
        (void*)(&value_list),
        (void*)(&siz_list),
        (void*)(&num),
        (void*)(&query),
        (void*)(&gen),
        (void*)(&tid)
    };

    for(int i=0;i<thread_number;i++){
        pthread_create(&threads[i], 0, txn_func, args);
    }

    for(int i=0;i<thread_number;i++){
        const char ** ret = new const char*;
        pthread_join(threads[i],(void**)ret);
        ASSERT_EQ(*ret, "OK");
        delete ret;
    }

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