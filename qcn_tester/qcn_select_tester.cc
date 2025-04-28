#include "qcn_select_tester.hh"

qcn_select_tester::qcn_select_tester(dbms_info& info, shared_ptr<schema> schema) 
: qcn_tester(info, schema){

    while (1) {
        try {
            initial_scope.new_stmt();
            auto select_query = make_shared<query_spec>((struct prod *)0, &initial_scope);
            query = select_query;
            
            ostringstream s;
            select_query->out(s);
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

void qcn_select_tester::eq_transform_query(shared_ptr<query_spec> select_query)
{
    if (schema::target_dbms != "clickhouse") { // clickhouse does not support subqueries in select list
        if (!select_query->has_group) {
            // transform the select list
            for (auto selected_value : select_query->select_list->value_exprs)
                selected_value->equivalent_transform();
        }
    }
    
    // transfrom the from clause
    auto ref_table = select_query->from_clause->reflist.back();
    eq_transform_table_ref(ref_table);
    
    // transform the where clause
    select_query->search->equivalent_transform();

    // transform the having clause
    if (select_query->has_group) {
        select_query->group_clause->having_cond_search->equivalent_transform();
    }
}

void qcn_select_tester::back_transform_query(shared_ptr<query_spec> select_query)
{
    if (schema::target_dbms != "clickhouse") {
        if (!select_query->has_group) {
            // back transform the select list
            for (auto selected_value : select_query->select_list->value_exprs)
                selected_value->back_transform();
        }
    }
    
    // back transfrom the from clause
    auto ref_table = select_query->from_clause->reflist.back();
    back_transform_table_ref(ref_table);
    
    // back transform the where clause
    select_query->search->back_transform();

    if (select_query->has_group) {
        select_query->group_clause->having_cond_search->back_transform();
    }
}

void qcn_select_tester::set_compid_for_query(shared_ptr<query_spec> select_query, int& start_id)
{
    if (schema::target_dbms != "clickhouse" && !select_query->has_group) {
        for (auto selected_value : select_query->select_list->value_exprs)
            selected_value->set_component_id(start_id);
    }

    auto ref_table = select_query->from_clause->reflist.back();
    set_compid_for_table_ref(ref_table, start_id);

    select_query->search->set_component_id(start_id);

    if (select_query->has_group) {
        select_query->group_clause->having_cond_search->set_component_id(start_id);
    }
}

// true: get the component
// false: cannot find the component
bool qcn_select_tester::get_comp_from_id_query(shared_ptr<query_spec> select_query, int id, shared_ptr<value_expr>& ret_comp)
{
    if (schema::target_dbms != "clickhouse" && !select_query->has_group) {
        for (auto selected_value : select_query->select_list->value_exprs) {
            GET_COMPONENT_FROM_ID_CHILD(id, ret_comp, selected_value); 
        }
    }

    auto ref_table = select_query->from_clause->reflist.back();
    if (get_comp_from_id_table(ref_table, id, ret_comp))
        return true;

    GET_COMPONENT_FROM_ID_CHILD(id, ret_comp, select_query->search); 

    if (select_query->has_group) {
        GET_COMPONENT_FROM_ID_CHILD(id, ret_comp, select_query->group_clause->having_cond_search);
    }

    return false;
}

bool qcn_select_tester::set_comp_from_id_query(shared_ptr<query_spec> select_query, int id, shared_ptr<value_expr> comp)
{
    if (schema::target_dbms != "clickhouse" && !select_query->has_group) {
        for (auto& selected_value : select_query->select_list->value_exprs) {
            SET_COMPONENT_FROM_ID_CHILD(id, comp, selected_value);
        }
    }

    auto ref_table = select_query->from_clause->reflist.back();
    if (set_comp_from_id_table(ref_table, id, comp))
        return true;
    
    auto bool_comp = dynamic_pointer_cast<bool_expr>(comp);
    if (bool_comp) {
        SET_COMPONENT_FROM_ID_CHILD(id, bool_comp, select_query->search);
        if (select_query->has_group) {
            SET_COMPONENT_FROM_ID_CHILD(id, bool_comp, select_query->group_clause->having_cond_search);
        }
    } else {
        if (select_query->search->set_component_from_id(id, comp))
            return true;
        if (select_query->has_group) {
            if (select_query->group_clause->having_cond_search->set_component_from_id(id, comp))
                return true;
        }
    }

    return false;
}

void qcn_select_tester::eq_transform_table_ref(shared_ptr<table_ref> table)
{
    if (auto pure_table = dynamic_pointer_cast<table_or_query_name>(table)) {
        return; // do nothing
    }
    if (auto subquery_table = dynamic_pointer_cast<table_subquery>(table)) {
        auto select_query = subquery_table->query;
        eq_transform_query(select_query);
        return;
    }
    if (auto join_table = dynamic_pointer_cast<joined_table>(table)) {
        eq_transform_table_ref(join_table->lhs);
        eq_transform_table_ref(join_table->rhs);
        return;
    }
}

void qcn_select_tester::back_transform_table_ref(shared_ptr<table_ref> table)
{
    if (auto pure_table = dynamic_pointer_cast<table_or_query_name>(table)) {
        return; // do nothing
    }
    if (auto subquery_table = dynamic_pointer_cast<table_subquery>(table)) {
        auto select_query = subquery_table->query;
        back_transform_query(select_query);
        return;
    }
    if (auto join_table = dynamic_pointer_cast<joined_table>(table)) {
        back_transform_table_ref(join_table->lhs);
        back_transform_table_ref(join_table->rhs);
        return;
    }
}

void qcn_select_tester::set_compid_for_table_ref(shared_ptr<table_ref> table, int& start_id)
{
    if (auto pure_table = dynamic_pointer_cast<table_or_query_name>(table)) {
        return; // do nothing
    }
    if (auto subquery_table = dynamic_pointer_cast<table_subquery>(table)) {
        auto select_query = subquery_table->query;
        set_compid_for_query(select_query, start_id);
        return;
    }
    if (auto join_table = dynamic_pointer_cast<joined_table>(table)) {
        set_compid_for_table_ref(join_table->lhs, start_id);
        set_compid_for_table_ref(join_table->rhs, start_id);
        return;
    }
}

bool qcn_select_tester::get_comp_from_id_table(shared_ptr<table_ref> table, int id, shared_ptr<value_expr>& ret_comp)
{
    if (auto pure_table = dynamic_pointer_cast<table_or_query_name>(table)) {
        return false; // do nothing
    }
    if (auto subquery_table = dynamic_pointer_cast<table_subquery>(table)) {
        auto select_query = subquery_table->query;
        if (get_comp_from_id_query(select_query, id, ret_comp))
            return true;
    }
    if (auto join_table = dynamic_pointer_cast<joined_table>(table)) {
        if (get_comp_from_id_table(join_table->lhs, id, ret_comp))
            return true;
        if (get_comp_from_id_table(join_table->rhs, id, ret_comp))
            return true;
    }
    return false;
}

bool qcn_select_tester::set_comp_from_id_table(shared_ptr<table_ref> table, int id, shared_ptr<value_expr> comp)
{
    if (auto pure_table = dynamic_pointer_cast<table_or_query_name>(table)) {
        return false; // do nothing
    }
    if (auto subquery_table = dynamic_pointer_cast<table_subquery>(table)) {
        auto select_query = subquery_table->query;
        if (set_comp_from_id_query(select_query, id, comp))
            return true;
    }
    if (auto join_table = dynamic_pointer_cast<joined_table>(table)) {
        if (set_comp_from_id_table(join_table->lhs, id, comp))
            return true;
        if (set_comp_from_id_table(join_table->rhs, id, comp))
            return true;
    }
    return false;
}

void qcn_select_tester::initial_origin_and_qit_query()
{
    ostringstream s1;
    query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    auto select_query = dynamic_pointer_cast<query_spec>(query);
    assert(select_query);
    eq_transform_query(select_query);
    
    ostringstream s2;
    select_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    back_transform_query(select_query);
}

// true: no bug
// false: trigger bug
bool qcn_select_tester::qcn_test()
{
    initial_origin_and_qit_query();
    return qcn_test_without_initialization();
}

bool qcn_select_tester::qcn_test_without_initialization()
{
    if (skip_one_original_execution == false) {
        try {
            execute_query(original_query, original_query_result);
        } catch (exception& e) { // should all success, but fail
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
    cerr << "compare select result: ";
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
            cerr << "qit results are different from original results, find logic bug!!" << endl;
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

void qcn_select_tester::save_testcase(string dir)
{
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) 
        make_dir_error_exit(dir);
    
    save_backup_file(dir, tested_dbms_info);
    save_query(dir, "select_origin.sql", original_query);
    save_query(dir, "select_qit.sql", qit_query);
    save_queries(dir, "env_stmts.sql", env_setting_stmts);    
}

void qcn_select_tester::minimize_testcase()
{
    // just simplify predicate for now
    auto select_query = dynamic_pointer_cast<query_spec>(query);
    int count = 0;
    
    set_compid_for_query(select_query, count);
    
    cerr << "number of predicate component: " << count << endl;
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(select_query, id, tmp_comp) == false) 
            continue;

        cout << "-----------------" << endl;
        cout << "replacing id " << id << endl;
        
        ostringstream sb;
        tmp_comp->out(sb);
        cout << "before, query component: " << sb.str() << endl;
        sb.clear();

        auto null_value = make_shared<const_bool>(query.get(), -1);
        null_value->component_id = id;

        auto set_status = set_comp_from_id_query(select_query, id, null_value);
        assert(set_status);

        if (qcn_test() == false) {
            cout << "successfully replace id " << id << " with null" << endl;
            cout << "-----------------" << endl;
            continue;
        }
        cout << "fail, reset the component" << endl;
        set_status = set_comp_from_id_query(select_query, id, tmp_comp);
        assert(set_status);
    }

    // minimize the qit query
    // initialize original query
    ostringstream s1;
    select_query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    eq_transform_query(select_query);
    // now cte_query is qit query
    
    for (int id = 0; id < count; id++) {
        shared_ptr<value_expr> tmp_comp;
        if (get_comp_from_id_query(select_query, id, tmp_comp) == false) 
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
    select_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    // both original_query and qit_query are in minimal size
}