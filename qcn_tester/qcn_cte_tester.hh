#ifndef QCN_CTE_TESTER_HH
#define QCN_CTE_TESTER_HH

#include "qcn_tester.hh"

struct qcn_cte_tester : qcn_tester {
    bool skip_one_original_execution;
    
    virtual bool qcn_test();
    virtual bool qcn_test_without_initialization();
    virtual void save_testcase(string dir);
    virtual void minimize_testcase();
    virtual void initial_origin_and_qit_query();
    
    qcn_cte_tester(dbms_info& info, shared_ptr<schema> schema);

    static void eq_transform_query(shared_ptr<common_table_expression> cte_query);
    static void back_transform_query(shared_ptr<common_table_expression> cte_query);

    static void set_compid_for_query(shared_ptr<common_table_expression> cte_query, int& start_id);
    static bool get_comp_from_id_query(shared_ptr<common_table_expression> cte_query, int id, shared_ptr<value_expr>& ret_comp);
    static bool set_comp_from_id_query(shared_ptr<common_table_expression> cte_query, int id, shared_ptr<value_expr> comp);
};

#endif