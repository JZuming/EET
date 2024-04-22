#pragma once

#include "value_expr.hh"

// just used to store the other expression's printed string
struct printed_expr : value_expr
{
    string printed_str;
    printed_expr(prod *p, shared_ptr<value_expr> expr);
    virtual void out(ostream &out);
    virtual ~printed_expr() {}
};