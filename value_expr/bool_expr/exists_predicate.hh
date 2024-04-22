#pragma once

#include "bool_expr.hh"

struct exists_predicate : bool_expr
{
    struct scope myscope;
    
    shared_ptr<prod> subquery;
    shared_ptr<bool_expr> eq_exer;

    // the information of original expr, used for
    vector<int> ori_columns;
    vector<relation> ori_derived_table;
    vector<vector<shared_ptr<value_expr>>> ori_value_exprs;
    virtual ~exists_predicate() {}
    exists_predicate(prod *p);
    exists_predicate(prod *p, shared_ptr<prod> subquery);
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};