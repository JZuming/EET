#include "qcn_delete_tester.hh"

qcn_delete_tester::qcn_delete_tester(dbms_info& info, shared_ptr<schema> schema) 
: qcn_tester(info, schema){

    while (1) {
        try {
            initial_scope.new_stmt();
            auto delete_query = make_shared<delete_stmt>((struct prod *)0, &initial_scope);
            query = delete_query;
            table_name = delete_query->victim->ident();
            
            ostringstream s;
            delete_query->out(s);
            original_query = s.str();
            s.clear();
        
            execute_get_changed_results(original_query, table_name, original_query_result, false);
            if (original_query_result.size() > MAX_PROCESS_ROW_NUM)
                continue;
        } catch (exception &e) { // unexpected error is handled inside execute_get_changed_results
            continue;
        }
        break;
    }
    
    skip_one_original_execution = true;
}

void qcn_delete_tester::eq_transform_query(shared_ptr<delete_stmt> delete_query)
{
    // transform the where clause
    delete_query->search->equivalent_transform();
}

void qcn_delete_tester::back_transform_query(shared_ptr<delete_stmt> delete_query)
{
    // transform the where clause
    delete_query->search->back_transform();
}

void qcn_delete_tester::initial_origin_and_qit_query()
{
    ostringstream s1;
    query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    auto delete_query = dynamic_pointer_cast<delete_stmt>(query);
    assert(delete_query);
    eq_transform_query(delete_query);
    
    ostringstream s2;
    delete_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    back_transform_query(delete_query);
}

// true: no bug
// false: trigger bug
bool qcn_delete_tester::qcn_test()
{
    initial_origin_and_qit_query();
    return qcn_test_without_initialization();
}

bool qcn_delete_tester::qcn_test_without_initialization()
{
    if (skip_one_original_execution == false) {
        try {
            execute_get_changed_results(original_query, table_name, original_query_result, false);
        } catch (exception& e) { // should all success, but fail
            return true;
        }
    } else {
        skip_one_original_execution = false;
    }

    try {
        execute_get_changed_results(qit_query, table_name, qit_query_result, false);
    } catch (exception& e) {
        cerr << "transform error: " << e.what() << endl;
        return true;
    }
    cerr << "compare delete result: ";
    cerr << "original: " << original_query_result.size() << ", ";
    cerr << "qit: " << qit_query_result.size() << endl;

    if (qit_query_result != original_query_result) {
        cerr << "validating the bug ... " << endl;

        try {
            execute_get_changed_results(original_query, table_name, original_query_result, false);
            execute_get_changed_results(qit_query, table_name, qit_query_result, false);
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

void qcn_delete_tester::save_testcase(string dir)
{
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) 
        make_dir_error_exit(dir);
    
    save_backup_file(dir, tested_dbms_info);
    save_query(dir, "delete_origin.sql", original_query);
    save_query(dir, "delete_qit.sql", qit_query);
}

void qcn_delete_tester::minimize_testcase()
{
    // just simplify predicate for now
    auto delete_query = dynamic_pointer_cast<delete_stmt>(query);
    int count = 0;
    
    set_compid_for_query(delete_query, count);
    
    cerr << "number of predicate component: " << count << endl;
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(delete_query, id, tmp_comp) == false) 
            continue;

        cout << "-----------------" << endl;
        cout << "replacing id " << id << endl;

        auto null_value = make_shared<const_bool>(query.get(), -1);
        null_value->component_id = id;

        auto set_status = set_comp_from_id_query(delete_query, id, null_value);
        assert(set_status);

        if (qcn_test() == false) {
            cout << "successfully replace id " << id << " with null" << endl;
            cout << "-----------------" << endl;
            continue;
        }

        set_status = set_comp_from_id_query(delete_query, id, tmp_comp);
        assert(set_status);
    }

    // minimize the qit query
    // initialize original query
    ostringstream s1;
    delete_query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    eq_transform_query(delete_query);
    // now cte_query is qit query
    
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(delete_query, id, tmp_comp) == false) 
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
    delete_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    // both original_query and qit_query are in minimal size
}

void qcn_delete_tester::set_compid_for_query(shared_ptr<delete_stmt> delete_query, int& start_id)
{
    delete_query->search->set_component_id(start_id);
}

bool qcn_delete_tester::get_comp_from_id_query(shared_ptr<delete_stmt> delete_query, int id, shared_ptr<value_expr>& ret_comp)
{
    GET_COMPONENT_FROM_ID_CHILD(id, ret_comp, delete_query->search);
    return false;
}

bool qcn_delete_tester::set_comp_from_id_query(shared_ptr<delete_stmt> delete_query, int id, shared_ptr<value_expr> comp)
{
    auto bool_comp = dynamic_pointer_cast<bool_expr>(comp);
    if (bool_comp) 
        SET_COMPONENT_FROM_ID_CHILD(id, bool_comp, delete_query->search);
    else {
        if (delete_query->search->set_component_from_id(id, comp))
            return true;
    }
    return false;
}