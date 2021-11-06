#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>
#include <utility>

//combine two id with single object
typedef std::pair<int, int64_t> page_id;

//structure for hashing pair object
//hash algorithm used in boost lib and std::hash
// https://www.boost.org/doc/libs/1_64_0/boost/functional/hash/hash.hpp
struct hash_pair {
  template <class T1, class T2>
  size_t operator()(const std::pair<T1, T2>& p) const{
    auto hash1 = std::hash<T1>{}(p.first);
    auto hash2 = std::hash<T2>{}(p.second);
    //use magic number and some bit shift to fit hash feature
    return hash1 ^ hash2 + 0x9e3779b9 + (hash2<<6) + (hash2>>2);
  }
};

//hash table that mapping page_id to lock object
std::unordered_map<page_id, lock_t*, hash_pair> hash_table;

//shared mutex object
pthread_mutex_t mutex;
pthread_mutexattr_t mattr;

//lock object
struct lock_t {
  lock_t* prev = nullptr; //prev pointer in lock list
  lock_t* nxt = nullptr; //next pointer in lock list
  lock_t* sentinel = nullptr; //sentinel(head) pointer
  pthread_cond_t cond = PTHREAD_COND_INITIALIZER; //conditional variable
};

typedef struct lock_t lock_t;

lock_t* find_lock_in_hash_table(int table_id, int64_t pagenum){
  page_id pid = {table_id, pagenum}; //make page_id to use as search key in hash table
  if(hash_table.find(pid)!=hash_table.end()){
      //found case
      //return corresponding lock object
      return hash_table[pid];
  }
  else{
      //not found case
      //return null
      return nullptr;
  }
}

//initialize lock table and mutex
int init_lock_table() {
  //initialize mutex attr
  pthread_mutexattr_init(&mattr);
  pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_DEFAULT);

  //initialize mutex
  pthread_mutex_init(&mutex, &mattr);

  //clear hash table
  hash_table.clear();
  return 0;
}

//allocate and append a new lock obejct to the lock list of
//the record having key
lock_t* lock_acquire(int table_id, int64_t key) {
  //begin critical section
  pthread_mutex_lock(&mutex);

  //get head lock from hash table
  lock_t* head = find_lock_in_hash_table(table_id,key);
  lock_t* ret = new lock_t; //make new lock
  if(head){
    //head lock existed case
    if(head->nxt){
      //predecessor's lock object existed case

      //connect with tail object
      head->prev->nxt = ret;
      ret->prev = head->prev;

      //connect with head object
      head->prev = ret;
      ret->sentinel = head;

      //wait for release
      pthread_cond_wait(&ret->cond, &mutex);
    }
    else{
      //no predecessor's lock case

      //connect with head object
      head->nxt = ret;
      head->prev = ret;
      ret->prev = head;
      ret->sentinel = head;
    }
  }
  else{
    //no head lock case

    //make new head object
    lock_t* head = new lock_t;

    //connect with head object
    head->nxt = ret;
    head->prev = ret;
    ret->prev = head;
    head->sentinel = ret->sentinel = head;

    //update hash table with new head object
    hash_table[{table_id,key}] = head;
  }
  //end critical section
  pthread_mutex_unlock(&mutex);
  //return lock object
  return ret;
};

//remove the lock_obj from the lock list
int lock_release(lock_t* lock_obj) {
  //begin critical section
  pthread_mutex_lock(&mutex);
  lock_obj->prev->nxt = lock_obj->nxt; // pop from list
  if(lock_obj->nxt){
    //successor's lock existed case

    //next conditional variable to signal
    pthread_cond_t* sig = &lock_obj->nxt->cond;
    //connect with prev object(head)
    lock_obj->nxt->prev = lock_obj->prev;
    //signal to wake up
    pthread_cond_signal(sig);
  }
  //delete object
  delete lock_obj;
  //end critical section
  pthread_mutex_unlock(&mutex);
  return 0;
}
