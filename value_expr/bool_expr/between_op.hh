#pragma once

#include "bool_expr.hh"

struct between_op : bool_expr
{
    shared_ptr<value_expr> lhs, rhs, mhs;
    bool use_eq_expr = false;
    shared_ptr<bool_expr> eq_expr;
    between_op(prod *p);
    virtual ~between_op(){};
    virtual void out(ostream &o);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};