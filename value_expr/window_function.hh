#pragma once

#include "value_expr.hh"
#include "column_reference.hh"
#include "win_funcall.hh"

struct window_function : value_expr
{
    virtual void out(std::ostream &out);
    virtual ~window_function() {}
    window_function(prod *p, sqltype *type_constraint);
    vector<shared_ptr<column_reference>> partition_by;
    vector<pair<shared_ptr<column_reference>, bool>> order_by;
    shared_ptr<win_funcall> aggregate;
    static bool allowed(prod *pprod);
    static bool disabled;
    virtual void accept(prod_visitor *v);
    // cannot transfer aggregate, othervwise aggregate will 
    // become other value_expr, which cause syntax error
};