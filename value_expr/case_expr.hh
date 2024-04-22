#pragma once

#include "value_expr.hh"
#include "bool_expr/bool_expr.hh"

struct case_expr : value_expr
{
    shared_ptr<bool_expr> condition;
    shared_ptr<value_expr> true_expr;
    shared_ptr<value_expr> false_expr;
    case_expr(prod *p, sqltype *type_constraint = 0);
    case_expr(prod *p, shared_ptr<bool_expr> c, shared_ptr<value_expr> t, shared_ptr<value_expr> f);
    virtual void out(ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};