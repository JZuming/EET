/// @file
/// @brief schema and dut classes for PostgreSQL

#ifndef POSTGRES_HH
#define POSTGRES_HH

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

#include <pqxx/pqxx>

extern "C" {
#include <libpq-fe.h>
}

#include <sys/time.h>
#include <fcntl.h>

#define POSTGRES_STMT_BLOCK_MS 200

#define OID long

struct pg_type : sqltype {
    OID oid_;
    char typdelim_;
    OID typrelid_;
    OID typelem_;
    OID typarray_;
    char typtype_;
    pg_type(string name,
        OID oid,
        char typdelim,
        OID typrelid,
        OID typelem,
        OID typarray,
        char typtype)
        : sqltype(name), oid_(oid), typdelim_(typdelim), typrelid_(typrelid),
          typelem_(typelem), typarray_(typarray), typtype_(typtype) { }
    virtual ~pg_type() {}
    virtual bool consistent(struct sqltype *rvalue);
};

struct pgsql_connection {
    PGconn *conn = 0;
    string test_db;
    unsigned int test_port;
    string inst_path;
    pgsql_connection(string db, unsigned int port);
    pgsql_connection(string db, unsigned int port, string path);
    ~pgsql_connection();
};

struct schema_pqxx : schema, pgsql_connection {
    map<OID, pg_type*> oid2type;
    map<string, pg_type*> name2type;

    virtual string quote_name(const string &id) {
        return id;
    }
    bool is_consistent_with_basic_type(sqltype *rvalue);
    schema_pqxx(string db, unsigned int port, string path, bool no_catalog);
    ~schema_pqxx();
};

struct dut_libpq : dut_base, pgsql_connection {
    virtual void test(const string &stmt,
                    vector<vector<string>>* output = NULL,
                    int* affected_row_num = NULL,
                    vector<string>* env_setting_stmts = NULL);
    virtual void reset(void);

    virtual void backup(void);
    virtual void reset_to_backup(void);

    virtual void get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content);

    static int save_backup_file(string db_name, string path);
    dut_libpq(string db, unsigned int port, string path);
};

#endif
