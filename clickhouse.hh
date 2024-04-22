#ifndef CLICKHOUSE_HH
#define CLICKHOUSE_HH

#include "schema.hh"
#include "relmodel.hh"
#include "dut.hh"
#include <iomanip>

using namespace std;

struct clickhouse_connection {
    string test_db;
    int test_port;
    clickhouse_connection(string db, int port);
    ~clickhouse_connection() {};
};

struct schema_clickhouse : schema, clickhouse_connection {
    schema_clickhouse(string db, int port);
    virtual string quote_name(const string &id) {
        return id;
    }
};

struct dut_clickhouse : dut_base, clickhouse_connection {
    string test_backup_file;
    
    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);
  
    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    dut_clickhouse(string db, int port, string backup_file);
};

#endif