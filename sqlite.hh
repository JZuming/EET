/// @file
/// @brief schema and dut classes for SQLite 3

#ifndef SQLITE_HH
#define SQLITE_HH

extern "C"  {
#include <sqlite3.h>
#include <unistd.h>
// #include <signal.h>
// #include <sys/time.h>
// #include <sys/wait.h>
// #include <sys/shm.h>
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
#include <cstring>
#include <iostream>
#include <set>
#include <stdexcept>
#include <cassert>

using namespace std;

struct sqlite_connection {
    sqlite3 *db;
    string db_file;
    char *zErrMsg = 0;
    int rc;
    void q(const char *query);
    sqlite_connection(std::string &conninfo);
    ~sqlite_connection();
};

struct schema_sqlite : schema, sqlite_connection {
    schema_sqlite(std::string &conninfo, bool no_catalog);\
    virtual void update_schema();
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_sqlite : dut_base, sqlite_connection {
    virtual void test(const std::string &stmt, 
        vector<vector<string>>* output = NULL, 
        int* affected_row_num = NULL,
        vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);
    
    virtual void backup(void);
    virtual void reset_to_backup(void);
    static int save_backup_file(string path, string db_name);

    virtual string commit_stmt();
    virtual string abort_stmt();
    virtual string begin_stmt();
    virtual string get_process_id();
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);
    dut_sqlite(std::string &conninfo);
};

#endif