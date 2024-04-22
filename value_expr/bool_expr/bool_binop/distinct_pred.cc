#include "distinct_pred.hh"

distinct_pred::distinct_pred(prod *p) : bool_binop(p)
{
    lhs = value_expr::factory(this);
    rhs = value_expr::factory(this, lhs->type);
}

void distinct_pred::out(std::ostream &o)
{
    OUTPUT_EQ_BOOL_EXPR(o);
    o << "(" << *lhs << ") is distinct from (" << *rhs << ")";
}