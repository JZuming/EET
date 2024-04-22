#pragma once

#include "bool_binop.hh"

struct distinct_pred : bool_binop
{
    distinct_pred(prod *p);
    virtual ~distinct_pred(){};
    virtual void out(ostream &o);
};