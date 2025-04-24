#include "dut.hh"
#include <fstream>
#include <sstream>

vector<string> process_dbrecord_into_sqls(string db_record_file)
{
    vector<string> stmt_queue;
    ifstream stmt_file(db_record_file);
    stringstream buffer;
    buffer << stmt_file.rdbuf();
    stmt_file.close();

    string stmts(buffer.str());
    int old_off = 0;
    string seperate_label = ";\n";
    while (1) {
        auto new_off = stmts.find(seperate_label, old_off);
        if (new_off == string::npos)
            break;
        
        auto each_sql = stmts.substr(old_off, new_off - old_off); // not include the seperate_label
        old_off = new_off + seperate_label.size();

        stmt_queue.push_back(each_sql + ";");
    }
    return stmt_queue;
}