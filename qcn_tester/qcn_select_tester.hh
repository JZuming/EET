#ifndef QCN_SELECT_TESTER_HH
#define QCN_SELECT_TESTER_HH

#include "qcn_tester.hh"

struct qcn_select_tester : qcn_tester {
    bool skip_one_original_execution;
    
    virtual bool qcn_test();
    virtual bool qcn_test_without_initialization();
    virtual void save_testcase(string dir);
    virtual void minimize_testcase();
    virtual void initial_origin_and_qit_query();
    
    qcn_select_tester(dbms_info& info, shared_ptr<schema> schema);
    
    static void eq_transform_table_ref(shared_ptr<table_ref> table);
    static void back_transform_table_ref(shared_ptr<table_ref> table);
    static void eq_transform_query(shared_ptr<query_spec> select_query);
    static void back_transform_query(shared_ptr<query_spec> select_query);

    static void set_compid_for_query(shared_ptr<query_spec> select_query, int& start_id);
    static void set_compid_for_table_ref(shared_ptr<table_ref> table, int& start_id);

    static bool get_comp_from_id_query(shared_ptr<query_spec> select_query, int id, shared_ptr<value_expr>& ret_comp);
    static bool get_comp_from_id_table(shared_ptr<table_ref> table, int id, shared_ptr<value_expr>& ret_comp);

    static bool set_comp_from_id_query(shared_ptr<query_spec> select_query, int id, shared_ptr<value_expr> comp);
    static bool set_comp_from_id_table(shared_ptr<table_ref> table, int id, shared_ptr<value_expr> comp);
};

#endif