#include "random.hh"

namespace smith {
  mt19937_64 rng;
}

map<string, struct file_random_machine*> file_random_machine::stream_map;
struct file_random_machine* file_random_machine::using_file = NULL;

int d6() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 6);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 6, 1);
}

int d9() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 9);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 9, 1);
}

int d12() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 12);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 12, 1);
}

int d20() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 20);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 20, 1);
}

int d42() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 42);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 42, 2);
}

int d100() {
    if (file_random_machine::using_file == NULL) {
        static uniform_int_distribution<> pick(1, 100);
        return pick(smith::rng);
    }
    else 
        return file_random_machine::using_file->get_random_num(1, 100, 2);
}

// 1 - x
// max: 0xffffff 3 bytes
int dx(int x) {
    if (file_random_machine::using_file == NULL) {
        uniform_int_distribution<> pick(1, x);
        return pick(smith::rng);
    }
    else {
        if (x == 1)
            return 1;
        int bytenum;
        if (x <= (0xff >> 3)) // 
            bytenum = 1;
        else if (x <= (0xffff >> 3))
            bytenum = 2;
        else
            bytenum = 3;
        return file_random_machine::using_file->get_random_num(1, x, bytenum);
    }
}


string random_string(int char_num) {
    string str = "";
    while (char_num > 0) {
        unsigned int rand_value = dx(0x100) - 1;
        char c = 32 + (rand_value % 95); // 32 - 127
        if (c == '\'' || c == '"' || c == '\\' || c == '%' || c == '_' || c == '\n') {
            // str = str + "\\";
            continue;
        }
        str = str + c;
        char_num--;
    }
    return str;
}

file_random_machine::file_random_machine(string s)
: filename(s)
{
    ifstream fin(filename, ios::binary);
    fin.seekg(0, ios::end);
    end_pos = fin.tellg();
    fin.seekg(0, ios::beg);

    if (end_pos == 0) {
        buffer = NULL;
        return;
    }
    
    if (end_pos < 100) {
        cerr << "Exit: rand file is too small (should larger than 100 byte)" << endl;
        exit(0);
    }
    buffer = new char[end_pos + 5];
    cur_pos = 0;
    read_byte = 0;

    fin.read(buffer, end_pos);
    fin.close();
}

file_random_machine::~file_random_machine()
{
    if (buffer != NULL)
        delete[] buffer;
}

struct file_random_machine *file_random_machine::get(string filename)
{
    if (stream_map.count(filename))
        return stream_map[filename];
    else
        return stream_map[filename] = new file_random_machine(filename);
}

bool file_random_machine::map_empty()
{
    return stream_map.empty();
}

void file_random_machine::use_file(string filename)
{
    using_file = get(filename);
}

int file_random_machine::get_random_num(int min, int max, int byte_num)
{
    static bool shown_all_used = false;

    if (buffer == NULL)
        return 0;
    
    auto scope = max - min + 1;
    if (scope <= 0) 
        return min;
    
    // default: small endian
    auto readable = end_pos - cur_pos;
    auto read_num = readable < byte_num ? readable : byte_num;
    int rand_num = 0;
    memcpy(&rand_num, buffer + cur_pos, read_num);
    cur_pos += read_num;

    if (cur_pos >= end_pos)
        cur_pos = 0;

    read_byte += read_num;

    if (read_byte > cur_pos && shown_all_used == false) {
        cerr << "WARNING: all bytes have been read" << endl;
        shown_all_used = true;
    }
    
    return min + rand_num % scope;
}