#include "const_expr.hh"

const_expr::const_expr(prod *p, sqltype *type_constraint)
    : value_expr(p), expr("")
{
    type = type_constraint ? type_constraint : scope->schema->inttype;

    if (d9() == 1)
    {
        if (scope->schema->target_dbms == "postgres")
            expr = string(scope->schema->null_literal) + "::" + type->name;
        else
            expr = scope->schema->null_literal;
        return;
    }

    if (type == scope->schema->inttype)
    {
        auto choice = d6();
        if (choice == 1)
            expr = "0";
        else if (choice == 2)
            expr = "-0";
        else if (choice <= 4 && schema::target_dbms != "clickhouse") // negative number easily cause type mismatch in CH (UInt and Int is not the same)
            expr = to_string(0 - d100());
        else
            expr = to_string(d100());
        return;
    }

    if (type == scope->schema->realtype)
    {
        auto choice = d6();
        if (choice == 1)
            expr = "0.0";
        else if (choice == 2)
            expr = "-0.0";
        else if (choice <= 4)
            expr = to_string(d100() - 1) + "." + to_string(d100());
        else
            expr = to_string(0 - d100()) + "." + to_string(d100());
        return;
    }

    if (type == scope->schema->texttype)
    {
        expr = "'" + random_string(d6() - 1) + "'";
        return;
    }

    if (type == scope->schema->datetype)
    {
        int year = 1970 + dx(137) - 1; // clickhouse support: 1970 - 2106
        int month = d12(); // 1 - 12
        int day = dx(28); // 1 - 28
        int hour = dx(24) - 1; // 0 - 23
        int minute = dx(60) - 1; // 0 - 59
        int second = dx(60) - 1; // 0 -59
        
        if (scope->schema->target_dbms == "clickhouse") {
            expr = "makeDateTime(";
            expr = expr + to_string(year) + ", ";
            expr = expr + to_string(month) + ", ";
            expr = expr + to_string(day) + ", ";
            expr = expr + to_string(hour) + ", ";
            expr = expr + to_string(minute) + ", ";
            expr = expr + to_string(second) + ")";
        }
        else if (scope->schema->target_dbms == "postgres") {
            expr = "make_timestamp(";
            expr = expr + to_string(year) + ", ";
            expr = expr + to_string(month) + ", ";
            expr = expr + to_string(day) + ", ";
            expr = expr + to_string(hour) + ", ";
            expr = expr + to_string(minute) + ", ";
            expr = expr + to_string(second) + ")";
        }
        else if (scope->schema->target_dbms == "mysql") {
            expr = "TIMESTAMP('";
            expr = expr + to_string(year) + "-";
            expr = expr + to_string(month) + "-";
            expr = expr + to_string(day) + "', '";
            expr = expr + to_string(hour) + ":";
            expr = expr + to_string(minute) + ":";
            expr = expr + to_string(second) + "')";
        }
        else
        {
            expr = "'" + to_string(year) + "-";
            expr = expr + (month < 10 ? "0" + to_string(month) : to_string(month)) + "-";
            expr = expr + (day < 10 ? "0" + to_string(day) : to_string(day)) + " ";
            expr = expr + (hour < 10 ? "0" + to_string(hour) : to_string(hour)) + ":";
            expr = expr + (minute < 10 ? "0" + to_string(minute) : to_string(minute)) + ":";
            expr = expr + (second < 10 ? "0" + to_string(second) : to_string(second)) + "'";
        }
        return;
    }

    if (scope->schema->target_dbms == "postgres")
        expr = string(scope->schema->null_literal) + "::" + type_constraint->name;
    else
        expr = scope->schema->null_literal;
}

void const_expr::out(ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }
    out << expr;
}