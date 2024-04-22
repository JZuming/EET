#pragma once

#include "value_expr.hh"

struct win_funcall : value_expr
{
    routine *proc;
    vector<shared_ptr<value_expr>> parms;
    virtual void out(std::ostream &out);
    virtual ~win_funcall() {}
    win_funcall(prod *p, sqltype *type_constraint = 0);
    virtual void accept(prod_visitor *v);

    virtual void equivalent_transform();
    virtual void back_transform();
    
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};