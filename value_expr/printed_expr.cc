#include "printed_expr.hh"

printed_expr::printed_expr(prod* p, shared_ptr<value_expr> expr)
    :value_expr(p)
{
    type = expr->type;
    
    ostringstream print_stream;
    expr->out(print_stream);
    printed_str = print_stream.str();
    return;
}

void printed_expr::out(ostream &out)
{
    out << printed_str;
}