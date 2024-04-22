#ifndef QCN_TESTER_HH
#define QCN_TESTER_HH

#include "../dbms_info.hh"
#include "../schema.hh"
#include "../prod.hh"
#include "../grammar.hh"
#include "../general_process.hh"

using namespace std;

#define MAX_PROCESS_ROW_NUM 10000 // 10k row
#define MIN_PROCESS_ROW_NUM 1     // 1 row
#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

struct qcn_tester {
    dbms_info tested_dbms_info;
    shared_ptr<schema> generated_db_schema;
    scope initial_scope;

    vector<string> env_setting_stmts;
    shared_ptr<prod> query; // could be select, update, delete, insert
    string original_query;
    string qit_query;

    multiset<row_output> original_query_result;
    multiset<row_output> qit_query_result;

    bool ignore_crash;

    static shared_ptr<bool_expr> bool_expr_wrapper(prod* p, shared_ptr<bool_expr> expr);
    static shared_ptr<bool_expr> bool_expr_extractor(shared_ptr<bool_expr> expr);

    virtual bool qcn_test() = 0;
    virtual bool qcn_test_without_initialization() = 0;
    virtual void save_testcase(string dir) = 0;
    virtual void minimize_testcase() = 0; // depends on the type of statements
    virtual void initial_origin_and_qit_query() = 0;

    void execute_query(string query, multiset<row_output>& result);
    void execute_get_changed_results(string query, string table_name, 
                            multiset<row_output>& result, bool is_after);
    void print_origin_qit_difference();
    void print_stmt_output(multiset<row_output>& stmt_output);
    
    bool simplify_qit_component_and_test(int component_id, shared_ptr<value_expr> target_expr);
    
    qcn_tester(dbms_info& info, shared_ptr<schema> schema);
};

#endif