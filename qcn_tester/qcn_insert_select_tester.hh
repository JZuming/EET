#ifndef QCN_INSERT_SELECT_TESTER_HH
#define QCN_INSERT_SELECT_TESTER_HH

#include "qcn_tester.hh"

struct qcn_insert_select_tester : qcn_tester {
    bool skip_one_original_execution;
    string table_name;

    virtual bool qcn_test();
    virtual bool qcn_test_without_initialization();
    virtual void save_testcase(string dir);
    virtual void minimize_testcase();
    virtual void initial_origin_and_qit_query();
    
    qcn_insert_select_tester(dbms_info& info, shared_ptr<schema> schema);

    static void eq_transform_query(shared_ptr<insert_select_stmt> insert_select_query);
    static void back_transform_query(shared_ptr<insert_select_stmt> insert_select_query);
};

#endif