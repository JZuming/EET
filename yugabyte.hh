#pragma once

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

#include <pqxx/pqxx>

extern "C" {
#include <libpq-fe.h>
}

#include <sys/time.h>
#include <fcntl.h>

#define OID long

struct yugabyte_type : sqltype {
    OID oid_;
    char typdelim_;
    OID typrelid_;
    OID typelem_;
    OID typarray_;
    char typtype_;
    yugabyte_type(string name,
        OID oid,
        char typdelim,
        OID typrelid,
        OID typelem,
        OID typarray,
        char typtype)
        : sqltype(name), oid_(oid), typdelim_(typdelim), typrelid_(typrelid),
          typelem_(typelem), typarray_(typarray), typtype_(typtype) { }
    virtual ~yugabyte_type() {}
    virtual bool consistent(struct sqltype *rvalue);
    // bool consistent_(sqltype *rvalue);
};

struct yugabyte_connection {
    PGconn *conn = 0;
    string test_db;
    unsigned int test_port;
    string host_addr;
    yugabyte_connection(string db, unsigned int port, string host);
    ~yugabyte_connection();
};

struct schema_yugabyte : schema, yugabyte_connection {
    map<OID, yugabyte_type*> oid2type;
    map<string, yugabyte_type*> name2type;

    virtual string quote_name(const string &id) {
        return id;
    }
    bool is_consistent_with_basic_type(sqltype *rvalue);
    // schema_pqxx(string &conninfo, bool no_catalog);
    schema_yugabyte(string db, unsigned int port, string host, bool no_catalog);
    ~schema_yugabyte();
};

// struct dut_pqxx : dut_base {
//     pqxx::connection c;
//     virtual void test(const std::string &stmt);
//     dut_pqxx(std::string conninfo);
// };

struct dut_yugabyte : dut_base, yugabyte_connection {
    virtual void test(const string &stmt, 
                    vector<vector<string>>* output = NULL, 
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);    

    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    static int save_backup_file(string db_name, string path);
    dut_yugabyte(string db, unsigned int port, string host);
};
