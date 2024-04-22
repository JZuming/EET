#include "qcn_insert_select_tester.hh"
#include "qcn_select_tester.hh"

qcn_insert_select_tester::qcn_insert_select_tester(dbms_info& info, shared_ptr<schema> schema) 
: qcn_tester(info, schema){

    while (1) {
        try {
            initial_scope.new_stmt();
            auto insert_select_query = make_shared<insert_select_stmt>((struct prod *)0, &initial_scope);
            query = insert_select_query;
            table_name = insert_select_query->victim->ident();
            
            ostringstream s;
            insert_select_query->out(s);
            original_query = s.str();
            s.clear();
        
            execute_get_changed_results(original_query, table_name, original_query_result, true);
            if (original_query_result.size() > MAX_PROCESS_ROW_NUM)
                continue;
        } catch (exception &e) { // unexpected error is handled inside execute_get_changed_results
            continue;
        }
        break;
    }
    
    skip_one_original_execution = true;
}

void qcn_insert_select_tester::eq_transform_query(shared_ptr<insert_select_stmt> insert_select_query)
{
    qcn_select_tester::eq_transform_query(insert_select_query->target_subquery);
}

void qcn_insert_select_tester::back_transform_query(shared_ptr<insert_select_stmt> insert_select_query)
{
    qcn_select_tester::back_transform_query(insert_select_query->target_subquery);
}

void qcn_insert_select_tester::initial_origin_and_qit_query()
{
    ostringstream s1;
    query->out(s1);
    original_query = s1.str();
    s1.clear();
    
    auto insert_select_query = dynamic_pointer_cast<insert_select_stmt>(query);
    assert(insert_select_query);
    eq_transform_query(insert_select_query);
    
    ostringstream s2;
    insert_select_query->out(s2);
    qit_query = s2.str();
    s2.clear();

    back_transform_query(insert_select_query);
}

// true: no bug
// false: trigger bug
bool qcn_insert_select_tester::qcn_test()
{
    initial_origin_and_qit_query();
    return qcn_test_without_initialization();
}

bool qcn_insert_select_tester::qcn_test_without_initialization()
{
    if (skip_one_original_execution == false) {
        try {
            execute_get_changed_results(original_query, table_name, original_query_result, true);
        } catch (exception& e) { // should all success, but fail
            return true;
        }
    } else {
        skip_one_original_execution = false;
    }

    try {
        execute_get_changed_results(qit_query, table_name, qit_query_result, true);
    } catch (exception& e) {
        cerr << "transform error: " << e.what() << endl;
        return true;
    }
    cerr << "compare insert select result: ";
    cerr << "original: " << original_query_result.size() << ", ";
    cerr << "qit: " << qit_query_result.size() << endl;

    if (qit_query_result != original_query_result) {
        cerr << "validating the bug ... " << endl;
        if (tested_dbms_info.dbms_name == "tidb") {
            string tidb_no_timeout = "SET MAX_EXECUTION_TIME = 600000;";
            execute_query(tidb_no_timeout, qit_query_result); // disable timeout
        }
        execute_get_changed_results(original_query, table_name, original_query_result, true);
        execute_get_changed_results(qit_query, table_name, qit_query_result, true);

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

void qcn_insert_select_tester::save_testcase(string dir)
{
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) 
        make_dir_error_exit(dir);
    
    save_backup_file(dir, tested_dbms_info);
    save_query(dir, "insert_select_origin.sql", original_query);
    save_query(dir, "insert_select_qit.sql", qit_query);
}

void qcn_insert_select_tester::minimize_testcase()
{
    return; // insert_select produce non-determined results, deprecated
}