#include "funcall.hh"

funcall::funcall(prod *p, sqltype *type_constraint, bool agg)
    : value_expr(p), is_aggregate(agg)
{
    if (type_constraint == scope->schema->internaltype)
        fail("cannot call functions involving internal type");

    auto &idx = agg ? p->scope->schema->aggregates_returning_type : p->scope->schema->routines_returning_type;

retry:

    if (!type_constraint)
    {
        proc = random_pick(idx.begin(), idx.end())->second;
    }
    else
    {
        auto iters = idx.equal_range(type_constraint);
        proc = random_pick<>(iters)->second;
        if (proc && !type_constraint->consistent(proc->restype))
        {
            retry();
            goto retry;
        }
    }

    if (!proc)
    {
        retry();
        goto retry;
    }

    if (type_constraint)
        type = type_constraint;
    else
        type = proc->restype;

    if (type == scope->schema->internaltype)
    {
        retry();
        goto retry;
    }

    for (auto type : proc->argtypes)
        if (type == scope->schema->internaltype || type == scope->schema->arraytype)
        {
            retry();
            goto retry;
        }

    for (auto argtype : proc->argtypes)
    {
        auto expr = value_expr::factory(this, argtype);
        assert(argtype->consistent(expr->type));
        parms.push_back(expr);
    }
}

void funcall::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }

    out << proc->ident() << "(";
    for (auto expr = parms.begin(); expr != parms.end(); expr++)
    {
        out << **expr;
        if (expr + 1 != parms.end())
        {
            out << ",";
            indent(out);
        }
    }

    if (is_aggregate && (parms.begin() == parms.end()))
        out << "*";

    out << ")";
}

void funcall::accept(prod_visitor *v)
{
    v->visit(this);
    for (auto p : parms)
        p->accept(v);
}

void funcall::equivalent_transform()
{
    value_expr::equivalent_transform();
    
    if (schema::target_dbms == "clickhouse")
        return;

    for (auto &parm : parms)
        parm->equivalent_transform();
}

void funcall::back_transform()
{
    value_expr::back_transform();
    
    if (schema::target_dbms == "clickhouse")
        return;
    
    for (auto &parm : parms)
        parm->back_transform();
}

void funcall::set_component_id(int &id)
{
    value_expr::set_component_id(id);
    for (auto &parm : parms)
        parm->set_component_id(id);
}

bool funcall::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    for (auto &parm : parms)
        GET_COMPONENT_FROM_ID_CHILD(id, component, parm);
    return value_expr::get_component_from_id(id, component);
}

bool funcall::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    for (auto &parm : parms)
        SET_COMPONENT_FROM_ID_CHILD(id, component, parm);
    return value_expr::set_component_from_id(id, component);
}