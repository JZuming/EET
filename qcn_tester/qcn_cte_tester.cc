#include "qcn_cte_tester.hh"
#include "qcn_select_tester.hh"

qcn_cte_tester::qcn_cte_tester(dbms_info& info, shared_ptr<schema> schema) 
: qcn_tester(info, schema){

    while (1) {
        try {
            initial_scope.new_stmt();
            auto cte_query = make_shared<common_table_expression>((struct prod *)0, &initial_scope);
            query = cte_query;
            
            ostringstream s;
            cte_query->out(s);
            original_query = s.str();
            s.clear();
        
            execute_query(original_query, original_query_result);
            if (original_query_result.size() > MAX_PROCESS_ROW_NUM)
                continue;
        } catch (exception &e) { // unexpected error is handled inside execute_query
            continue;
        }
        break;
    }
    
    skip_one_original_execution = true;
}

void qcn_cte_tester::eq_transform_query(shared_ptr<common_table_expression> cte_query)
{
    for (auto query:cte_query->with_queries)
        qcn_select_tester::eq_transform_query(query);
    qcn_select_tester::eq_transform_query(cte_query->query);
}

void qcn_cte_tester::back_transform_query(shared_ptr<common_table_expression> cte_query)
{
    for (auto query:cte_query->with_queries)
        qcn_select_tester::back_transform_query(query);
    qcn_select_tester::back_transform_query(cte_query->query);
}

void qcn_cte_tester::initial_origin_and_qit_query()
{
    ostringstream s1;
    query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    auto cte_query = dynamic_pointer_cast<common_table_expression>(query);
    assert(cte_query);
    eq_transform_query(cte_query);
    
    ostringstream s2;
    cte_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    back_transform_query(cte_query);
}

// true: no bug
// false: trigger bug
bool qcn_cte_tester::qcn_test()
{
    initial_origin_and_qit_query();
    return qcn_test_without_initialization();
}

bool qcn_cte_tester::qcn_test_without_initialization()
{
    if (skip_one_original_execution == false) {
        try {
            execute_query(original_query, original_query_result);
        } catch (exception& e) { // should all success, but fail
            cerr << "basic query error: " << e.what() << endl;
            return true;
        }
    } else {
        skip_one_original_execution = false;
    }

    try {
        execute_query(qit_query, qit_query_result);
    } catch (exception& e) {
        cerr << "transform error: " << e.what() << endl;
        return true;
    }
    cerr << "compare cte result: ";
    cerr << "original: " << original_query_result.size() << ", ";
    cerr << "qit: " << qit_query_result.size() << endl;

    if (qit_query_result != original_query_result) {
        cerr << "validating the bug ... " << endl;

        try {
            execute_query(original_query, original_query_result);
            execute_query(qit_query, qit_query_result);
        } catch (exception& e) {
            cerr << "error when validating: " << e.what() << endl;
            return true;
        }

        if (qit_query_result != original_query_result) {
            cerr << "qit results are different from original results, find logical bug!!" << endl;
            cerr << "original_query_result: " << original_query_result.size() << endl;
            cerr << "qit_query_result: " << qit_query_result.size() << endl;
            return false;
        }
        cerr << "not real bug ... " << endl;
        return true;
    }
    else
        return true;
}

void qcn_cte_tester::save_testcase(string dir)
{
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) 
        make_dir_error_exit(dir);
    
    save_backup_file(dir, tested_dbms_info);
    save_query(dir, "cte_origin.sql", original_query);
    save_query(dir, "cte_qit.sql", qit_query);
}

void qcn_cte_tester::minimize_testcase()
{
    // just simplify predicate for now
    auto cte_query = dynamic_pointer_cast<common_table_expression>(query);
    int count = 0;
    
    set_compid_for_query(cte_query, count);
    
    cerr << "number of predicate component: " << count << endl;
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(cte_query, id, tmp_comp) == false) 
            continue;

        cout << "-----------------" << endl;
        cout << "replacing id " << id << endl;

        auto null_value = make_shared<const_bool>(query.get(), -1);
        null_value->component_id = id;

        auto set_status = set_comp_from_id_query(cte_query, id, null_value);
        assert(set_status);

        if (qcn_test() == false) {
            cout << "successfully replace id " << id << " with null" << endl;
            cout << "-----------------" << endl;
            continue;
        }

        set_status = set_comp_from_id_query(cte_query, id, tmp_comp);
        assert(set_status);
    }

    // minimize the qit query
    // initialize original query
    ostringstream s1;
    cte_query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    eq_transform_query(cte_query);
    // now cte_query is qit query
    
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(cte_query, id, tmp_comp) == false) 
            continue;
        
        if (tmp_comp->is_transformed == false)
            continue;
        
        cout << "-----------------" << endl;
        cout << "back transaform id: " << id << endl;
        ostringstream sb;
        tmp_comp->out(sb);
        cout << "before, qit component: " << sb.str() << endl;
        sb.clear();
        
        tmp_comp->back_transform();
        
        ostringstream s;
        query->out(s);
        qit_query = s.str();
        s.clear();

        if (qcn_test_without_initialization() == false) {
            cout << "successfully back transaform id " << id << endl;
            ostringstream sa;
            tmp_comp->out(sa);
            cout << "after, qit component: " << sa.str() << endl;
            sa.clear();
            cout << "-----------------" << endl;
            continue;
        }

        tmp_comp->equivalent_transform();
    }

    ostringstream s2;
    cte_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    // both original_query and qit_query are in minimal size
}

void qcn_cte_tester::set_compid_for_query(shared_ptr<common_table_expression> cte_query, int& start_id)
{
    qcn_select_tester::set_compid_for_query(cte_query->query, start_id);
    
    for (auto& query : cte_query->with_queries) {
        qcn_select_tester::set_compid_for_query(query, start_id);
    }

    return;
}

bool qcn_cte_tester::get_comp_from_id_query(shared_ptr<common_table_expression> cte_query, int id, shared_ptr<value_expr>& ret_comp)
{
    if (qcn_select_tester::get_comp_from_id_query(cte_query->query, id, ret_comp))
        return true;
    for (auto& query : cte_query->with_queries) {
        if (qcn_select_tester::get_comp_from_id_query(query, id, ret_comp))
            return true;
    }
    return false;
}

bool qcn_cte_tester::set_comp_from_id_query(shared_ptr<common_table_expression> cte_query, int id, shared_ptr<value_expr> comp)
{
    if (qcn_select_tester::set_comp_from_id_query(cte_query->query, id, comp))
        return true;
    for (auto& query : cte_query->with_queries) {
        if (qcn_select_tester::set_comp_from_id_query(query, id, comp))
            return true;
    }
    return false;
}