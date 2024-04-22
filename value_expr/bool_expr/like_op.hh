#pragma once

#include "bool_expr.hh"

struct like_op : bool_expr
{
    shared_ptr<value_expr> lhs;
    string like_operator; // like or not like
    string like_format;
    like_op(prod *p);
    virtual ~like_op(){};
    virtual void out(std::ostream &o);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};