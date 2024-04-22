#pragma once

#include "bool_expr.hh"
#include "../case_expr.hh"

struct in_query : bool_expr
{
    struct scope myscope;
    shared_ptr<value_expr> lhs;
    shared_ptr<prod> in_subquery;
    shared_ptr<case_expr> eq_expr;
    in_query(prod *p);
    in_query(prod *p, shared_ptr<value_expr> expr, shared_ptr<prod> subquery);
    virtual ~in_query(){};
    virtual void out(ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};