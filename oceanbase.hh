#pragma once

extern "C"  {
#include <mysql/mysql.h>
}

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"

#include <sys/time.h> // for gettimeofday

#define MYSQL_STMT_BLOCK_MS 100

struct ob_connection {
    MYSQL conn;
    string test_db;
    unsigned int test_port;
    string host_addr;
    ob_connection(string db, unsigned int port, string host);
    ~ob_connection();
};

struct schema_ob : schema, ob_connection {
    schema_ob(string db, unsigned int port, string host);
    virtual void update_schema();
    virtual std::string quote_name(const std::string &id) {
        return id;
    }
};

struct dut_ob : dut_base, ob_connection {
    virtual void test(const string &stmt, 
        vector<vector<string>>* output = NULL, 
        int* affected_row_num = NULL,
        vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
    
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    static pid_t fork_db_server();
    dut_ob(string db, unsigned int port, string host);

    static int save_backup_file(string testdb, string path);
};