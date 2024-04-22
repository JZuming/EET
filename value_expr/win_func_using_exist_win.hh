#pragma once

#include "value_expr.hh"
#include "win_funcall.hh"

struct win_func_using_exist_win : value_expr
{
    virtual void out(std::ostream &out);
    virtual ~win_func_using_exist_win() {}
    win_func_using_exist_win(prod *p, sqltype *type_constraint, string exist_win);
    shared_ptr<win_funcall> aggregate;
    string exist_window;
    virtual void accept(prod_visitor *v);
    // cannot transfer aggregate, othervwise aggregate will 
    // become other value_expr, which cause syntax error
};
