#pragma once

#include "value_expr.hh"

struct atomic_subselect : value_expr
{
    named_relation *tab;
    column *col;
    int offset;
    routine *agg;
    atomic_subselect(prod *p, sqltype *type_constraint = 0);
    virtual void out(std::ostream &out);
};