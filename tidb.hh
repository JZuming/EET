/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef TIDB_HH
#define TIDB_HH

extern "C"  {
#include <mysql/mysql.h>
#include <unistd.h>
}

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <iomanip>
#include <iostream>
#include <set>
#include <stdexcept>
#include <cassert>
#include <cstring>

using namespace std;

struct tidb_connection {
    MYSQL mysql;
    string test_db;
    unsigned int test_port;
    tidb_connection(string db, unsigned int port);
    ~tidb_connection();
};

struct schema_tidb : schema, tidb_connection {
    schema_tidb(string db, unsigned int port);
    virtual void update_schema();
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_tidb : dut_base, tidb_connection {
    virtual void test(const std::string &stmt, 
        vector<vector<string>>* output = NULL, 
        int* affected_row_num = NULL,
        vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);

    // must be static, because db server may crash, and thus cannot connect again
    static int save_backup_file(string testdb, string path);
    
    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();

    static pid_t fork_db_server();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_tidb(string db, unsigned int port);
};

#endif