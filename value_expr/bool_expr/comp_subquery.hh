#pragma once

#include "bool_expr.hh"

struct comp_subquery : bool_expr
{
    struct scope myscope;
    shared_ptr<value_expr> lhs;
    string comp_op; // =  >  <  >=  <=  <>
    shared_ptr<prod> target_subquery;

    comp_subquery(prod *p);
    virtual ~comp_subquery(){};
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};