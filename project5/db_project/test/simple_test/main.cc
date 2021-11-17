#include "api.h"

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <ctime>
#include <algorithm>
#include <vector>
#include <random>
#include <unistd.h>
#include <utility>

using namespace std;

static const std::string CHARACTERS {
	"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};

static const std::string BASE_TABLE_NAME { "table" };
static constexpr int NUM_TABLES { 1 };

static constexpr int NUM_KEYS { 10000 };

static constexpr int MIN_VAL_SIZE { 50 };
static constexpr int MAX_VAL_SIZE { 112 };

int main( int argc, char ** argv ) {

	int64_t table_id;
	int64_t key;
	char* value;
	uint16_t size;

	char ret_value[MAX_VAL_SIZE];
	uint16_t ret_size;

	std::vector<std::pair<int64_t, std::string>> key_value_pairs {};
	std::vector<std::pair<std::string, int64_t>> tables {};

	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<uint16_t> len_dis(MIN_VAL_SIZE, MAX_VAL_SIZE);
	std::uniform_int_distribution<int> char_dis(1, CHARACTERS.size());

	std::default_random_engine rng(rd());

	tables.reserve(NUM_TABLES);
	key_value_pairs.reserve(NUM_KEYS);

	auto helper_function = [] (auto& gen, auto& cd, auto& size) -> std::string {
		std::string ret_str;
		int index;
		ret_str.reserve(size);

		for (int i = 0; i < size; ++i) {
			index = cd(gen) - 1;
			ret_str += CHARACTERS[index];
		}
		return ret_str;
	};

	for (int i = 0; i < NUM_TABLES; ++i)
		tables.emplace_back(BASE_TABLE_NAME + to_string(i), 0);

	for (int i = 1; i <= NUM_KEYS; ++i) {
		size = len_dis(gen);
		key_value_pairs.emplace_back(i, helper_function(gen, char_dis, size));
	}


	// Simple test code.
	std::cout << "[INIT START]\n";
	if (init_db() != 0) {
		return 0;
	}
	std::cout << "[INIT END]\n\n";
	std::cout << "[OPEN TABLE START]\n";
	for (auto& t : tables) {
		table_id = open_table(const_cast<char*>(t.first.c_str()));
		if (table_id < 0) {
			goto func_exit;
		} else {
			t.second = table_id;
		}
	}
	std::cout << "[OPEN TABLE END]\n\n";
	std::cout << "[TEST START]\n";
	for (const auto& t: tables) {
		std::cout << "[TABLE : " << t.first << " START]\n";
		std::shuffle(key_value_pairs.begin(), key_value_pairs.end(), rng);
		std::cout << "[INSERT START]\n";
		for (const auto& kv: key_value_pairs) {
			if (db_insert(t.second, kv.first, const_cast<char*>(kv.second.c_str()), kv.second.size()) != 0) {
				goto func_exit;
			}
		}
		std::cout << "[INSERT END]\n";
		std::cout << "[FIND START]\n";
		for (const auto& kv: key_value_pairs) {
			ret_size = 0;
			memset(ret_value, 0x00, MAX_VAL_SIZE);
			if (db_find(t.second, kv.first, ret_value, &ret_size) != 0) {
				goto func_exit;
			} else if (kv.second.size() != ret_size ||
					kv.second != std::string(ret_value, ret_size)) {
				goto func_exit;
			}
		}
		std::cout << "[FIND END]\n";
		std::cout << "[DELETE START]\n";
		for (const auto& kv: key_value_pairs) {
			if (db_delete(t.second, kv.first) != 0) {
				goto func_exit;
			}
		}
		std::cout << "[DELETE END]\n";
		std::cout << "[FIND START AGAIN]\n";
		for (const auto& kv: key_value_pairs) {
			ret_size = 0;
			memset(ret_value, 0x00, MAX_VAL_SIZE);
			if (db_find(t.second, kv.first, ret_value, &ret_size) == 0) {
				goto func_exit;
			}
		}
		std::cout << "[FIND END AGAIN]\n";
		std::cout << "[TABLE : " << t.first << " END]\n\n";
	}
	std::cout << "[TEST END]\n";

func_exit:

	std::cout << "[SHUTDOWN START]\n";
	if (shutdown_db() != 0) {
		return 0;
	}
	std::cout << "[SHUTDOWN END]\n";
	return 0;
}
