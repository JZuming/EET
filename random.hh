/// @file
/// @brief randomness

#ifndef RANDOM_HH
#define RANDOM_HH

// #define __FILE_RANDOM__
// #define __RAND_FILE_NAME__ "rand_file.txt"
// #define __READ_BYTE_NUM__ "read_byte.txt"

#include <random>
#include <utility>
#include <stdexcept>
#include <iterator>
#include <fstream>
#include <map>
#include <string>
#include <memory>
#include <iostream>
#include <cstring>

using namespace std;

namespace smith {
  extern mt19937_64 rng;
}

int d6(), d9(), d12(), d20(), d42(), d100();
string random_string(int char_num);
int dx(int x);

struct file_random_machine {
    string filename;
    char * buffer;
    int cur_pos;
    int end_pos;
    int read_byte;

    static struct file_random_machine *using_file;
    static map<string, struct file_random_machine*> stream_map;
    static struct file_random_machine *get(string filename);
    static bool map_empty();
    static void use_file(string filename);

    file_random_machine(string s);
    ~file_random_machine();
    int get_random_num(int min, int max, int byte_num);
};

template<typename T> T& random_pick(vector<T>& container) {
    if (!container.size()) {
        throw runtime_error("No candidates available (random_pick)");
    }

    if (file_random_machine::using_file == NULL) {
        uniform_int_distribution<int> pick(0, container.size()-1);
        return container[pick(smith::rng)];
    }
    else
        return container[dx(container.size()) - 1];
}

template<typename I>
I random_pick(I beg, I end) {
    if (beg == end)
      throw runtime_error("No candidates available (random_pick)");

    if (file_random_machine::using_file == NULL) {
        uniform_int_distribution<> pick(0, distance(beg, end) - 1);
        advance(beg, pick(smith::rng));
        return beg;
    }
    else {
        advance(beg, dx(distance(beg, end)) - 1);
        return beg;
    }
}

template<typename I>
I random_pick(pair<I,I> iters) {
  return random_pick(iters.first, iters.second);
}

#endif
