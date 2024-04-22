#pragma once

#include "../prod.hh"
#include "../relmodel.hh"
#include "../schema.hh"
#include <string>

using namespace std;

#define SET_COMPONENT_FROM_ID_CHILD(id, component, child) \
    do                                                    \
    {                                                     \
        if (child->component_id == id)                    \
        {                                                 \
            child = component;                            \
            child->component_id = id;                     \
            return true;                                  \
        }                                                 \
        if (child->set_component_from_id(id, component))  \
            return true;                                  \
    } while (0)

#define GET_COMPONENT_FROM_ID_CHILD(id, component, child)     \
    do                                                        \
    {                                                         \
        if (child->component_id == id)                        \
        {                                                     \
            component = child;                                \
            return true;                                      \
        }                                                     \
        else if (child->get_component_from_id(id, component)) \
            return true;                                      \
    } while (0)

struct value_expr : prod
{
    sqltype *type = NULL;

    // component used for transaformation
    bool is_transformed = false;
    shared_ptr<value_expr> eq_value_expr;
    bool has_print_eq_expr = false; // used to prevent recursively print eq_expr (as eq_expr reference this value_expr itself)

    int component_id = -1; // used for reduction

    virtual void out(ostream &out) = 0;
    virtual ~value_expr() {}
    value_expr(prod *p) : prod(p) {}
    static shared_ptr<value_expr> factory(prod *p, sqltype *type_constraint = 0,
                                          vector<shared_ptr<named_relation>> *prefer_refs = 0);

    virtual void equivalent_transform();
    virtual void back_transform();
    void out_eq_value_expr(ostream &out);

    // for reduction
    void mark_transformed();
    void unmark_transformed();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component) {
        (void)id; (void)component;
        return false;
    } // cannot return itself
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component) {
        (void)id; (void)component;
        return false;
    } // cannot set itself
};