#pragma once

#include "value_expr.hh"

struct const_expr : value_expr
{
    string expr;
    const_expr(prod *p, sqltype *type_constraint = NULL);
    const_expr(prod *p, string specified_value) : value_expr(p), expr(specified_value){};
    virtual void out(ostream &out);
    virtual ~const_expr() {}
};