#pragma once

#include "bool_expr.hh"

struct const_bool : bool_expr
{
    virtual ~const_bool() {}
    const char *op;
    shared_ptr<bool_expr> eq_expr;
    const_bool(prod *p);
    const_bool(prod *p, int is_true);
    virtual void equivalent_transform();
    virtual void out(std::ostream &out);
};