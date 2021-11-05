#include "lock_table.h"
#include <pthread.h>
#include <unordered_map>
#include <utility>

typedef std::pair<int, int64_t> page_id;

struct hash_pair {
  template <class T1, class T2>
  size_t operator()(const std::pair<T1, T2>& p) const{
    auto hash1 = std::hash<T1>{}(p.first);
    auto hash2 = std::hash<T2>{}(p.second);
    //use magic number and some bit shift to fit hash feature
    return hash1 ^ hash2 + 0x9e3779b9 + (hash2<<6) + (hash2>>2);
  }
};

std::unordered_map<page_id, lock_t*, hash_pair> hash_table;

pthread_mutex_t mutex;
pthread_mutexattr_t mattr;

struct lock_t {
  lock_t* prev = nullptr;
  lock_t* nxt = nullptr;
  lock_t* sentinel = nullptr;
  pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
};

typedef struct lock_t lock_t;

lock_t* find_lock_in_hash_table(int table_id, int64_t pagenum){
  page_id pid = {table_id, pagenum}; //make page_id to use as search key in hash table
  if(hash_table.find(pid)!=hash_table.end()){
      //found case
      //return corresponding lock_t
      return hash_table[pid];
  }
  else{
      //not found case
      //return null
      return nullptr;
  }
}

int init_lock_table() {
  pthread_mutexattr_init(&mattr);
  pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_DEFAULT);
  pthread_mutex_init(&mutex, &mattr);
  hash_table.clear();
  return 0;
}

lock_t* lock_acquire(int table_id, int64_t key) {
  pthread_mutex_lock(&mutex);
  lock_t* head = find_lock_in_hash_table(table_id,key);
  lock_t* ret = new lock_t;
  if(head){
    if(head->nxt){
      head->prev->nxt = ret;
      ret->prev = head->prev;
      head->prev = ret;
      ret->sentinel = head;
      pthread_cond_wait(&ret->cond, &mutex);
    }
    else{
      head->nxt = ret;
      head->prev = ret;
      ret->prev = head;
      ret->sentinel = head;
    }
  }
  else{
    lock_t* head = new lock_t;
    head->nxt = ret;
    head->prev = ret;
    ret->prev = head;
    head->sentinel = ret->sentinel = head;
    hash_table[{table_id,key}] = head;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
};

int lock_release(lock_t* lock_obj) {
  pthread_mutex_lock(&mutex);
  lock_t* head = lock_obj->sentinel;
  lock_obj->prev->nxt = lock_obj->nxt;
  if(lock_obj->nxt){
    pthread_cond_t* sig = &lock_obj->nxt->cond;
    lock_obj->nxt->prev = lock_obj->prev;
    pthread_cond_signal(sig);
  }
  delete lock_obj;
  pthread_mutex_unlock(&mutex);
  return 0;
}
