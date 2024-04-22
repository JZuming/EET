#include "qcn_tester.hh"
#include <chrono>
#include <thread>

extern unsigned long long dbms_execution_ms;

// search -> not (False or not (True and search))
shared_ptr<bool_expr> qcn_tester::bool_expr_wrapper(prod* p, shared_ptr<bool_expr> expr)
{
    auto true_value = make_shared<const_bool>(p, 1);
    auto false_value = make_shared<const_bool>(p, 0);
    auto true_and_expr = make_shared<bool_term>(p, false, true_value, expr);
    auto not_1 = make_shared<not_expr>(p, true_and_expr);
    auto false_or_expr = make_shared<bool_term>(p, true, false_value, not_1);
    auto not_2 = make_shared<not_expr>(p, false_or_expr);

    return not_2;
}

// not (False or not (True and search)) -> search
shared_ptr<bool_expr> qcn_tester::bool_expr_extractor(shared_ptr<bool_expr> expr)
{
    auto not_2 = dynamic_pointer_cast<not_expr>(expr);
    auto false_or_expr = dynamic_pointer_cast<bool_term>(not_2->inner_expr);
    auto not_1 = dynamic_pointer_cast<not_expr>(false_or_expr->rhs);
    auto true_and_expr = dynamic_pointer_cast<bool_term>(not_1->inner_expr);
    return dynamic_pointer_cast<bool_expr>(true_and_expr->rhs);
}

void qcn_tester::execute_get_changed_results(string query, string table_name, 
                                        multiset<row_output>& result, bool is_after)
{
    vector<string> table_names;
    map<string, stmt_output> prev_content;
    multiset<row_output> prev_result;
    map<string, stmt_output> new_content;
    multiset<row_output> new_result;
    result.clear();

    auto execute_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(tested_dbms_info);
        dut->reset_to_backup();
        table_names.push_back(table_name);
        dut->get_content(table_names, prev_content);
        for (auto& item : prev_content[table_name]) 
            prev_result.insert(item);
    
        dut->test(query, NULL, NULL, &env_setting_stmts);

        dut->get_content(table_names, new_content);
        for (auto& item : new_content[table_name]) 
            new_result.insert(item);
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - execute_start);

    } catch(exception& e) {
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - execute_start);

        string err = e.what();
        bool expected = err.find("expected error") != string::npos;
        if (!expected && ignore_crash == false) {
            save_query(".", "unexpected.sql", query);
            save_backup_file(".", tested_dbms_info);
            cerr << "unexpected error: " << err << endl;
            abort(); // stop if trigger unexpected error
        } else if (!expected) {
            cerr << "unexpected error: " << err << endl;
            cerr << "option [ignore-crash] enabled, so sleep 1 min (wait for recovering the server) and skip this bug" << endl;
            chrono::minutes duration(1);
            this_thread::sleep_for(duration);
            cerr << "sleep over, restart testing" << endl;
            throw;
        }
        throw;
    }

    multiset<row_output>* target_result;
    multiset<row_output>* according_result;
    if (is_after) {
        target_result = &new_result;
        according_result = &prev_result;
    } else {
        target_result = &prev_result;
        according_result = &new_result;
    }

    for (auto& item : *target_result) {
        if (according_result->find(item) != according_result->end())
            continue; // skip the not changed item
        result.insert(item);
    }
}

void qcn_tester::execute_query(string query, multiset<row_output>& result)
{
    result.clear();
    vector<row_output> tmp_output;

    auto execute_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(tested_dbms_info);
        dut->test(query, &tmp_output, NULL, &env_setting_stmts);
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - execute_start);
        
    } catch(exception& e) {
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - execute_start);
        
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos);
        if (!expected && ignore_crash == false) {
            save_query(".", "unexpected.sql", query);
            save_backup_file(".", tested_dbms_info);
            cerr << "unexpected error: " << err << endl;
            abort(); // stop if trigger unexpected error
        } else if (!expected) {
            cerr << "unexpected error: " << err << endl;
            cerr << "option [ignore-crash] enabled, so sleep 1 min (wait for recovering the server) and skip this bug" << endl;
            chrono::minutes duration(1);
            this_thread::sleep_for(duration);
            cerr << "sleep over, restart testing" << endl;
            throw;
        }
        throw;
    }
    
    for (auto& item:tmp_output)
        result.insert(item);
}

qcn_tester::qcn_tester(dbms_info& info, shared_ptr<schema> schema) {
    ignore_crash = false;
    tested_dbms_info = info;
    generated_db_schema = schema;
    generated_db_schema->fill_scope(initial_scope);

    while (!generated_db_schema->supported_setting.empty() && d6() <= 4) {
        auto set_statement = make_shared<set_stmt>((struct prod *)0, &initial_scope);
        // don't test set statement, developer should guarentee it is always valid

        ostringstream s;
        set_statement->out(s);
        auto set_stmt_str = s.str();
        s.clear();
        env_setting_stmts.push_back(set_stmt_str);
    }
}

void qcn_tester::print_stmt_output(multiset<row_output>& stmt_output)
{
    for (auto& row : stmt_output) {
        for (auto& str : row) {
            cerr << str << " ";
        }
        cerr << endl;
    }
}

void qcn_tester::print_origin_qit_difference()
{
    multiset<row_output> row_only_in_origin;
    multiset<row_output> row_only_in_qit;

    for (auto& row_o : original_query_result) {
        if (qit_query_result.find(row_o) == qit_query_result.end())
            row_only_in_origin.insert(row_o);
    }

    for (auto& row_q : qit_query_result) {
        if (original_query_result.find(row_q) == original_query_result.end())
            row_only_in_qit.insert(row_q);
    }

    cerr << "row_only_in_origin:" << endl;
    print_stmt_output(row_only_in_origin);
    cerr << "row_only_in_qit:" << endl;
    print_stmt_output(row_only_in_qit);
}