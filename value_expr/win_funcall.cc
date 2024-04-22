#include "win_funcall.hh"

win_funcall::win_funcall(prod *p, sqltype *type_constraint)
    : value_expr(p)
{
    if (type_constraint == scope->schema->internaltype)
        fail("cannot call functions involving internal type");

    multimap<sqltype *, routine *> *idx;
    if (d6() > 4)
        idx = &p->scope->schema->aggregates_returning_type;
    else
        idx = &p->scope->schema->windows_returning_type;

retry:

    if (!type_constraint)
    {
        proc = random_pick(idx->begin(), idx->end())->second;
    }
    else
    {
        auto iters = idx->equal_range(type_constraint);
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
        assert(argtype);
        auto expr = value_expr::factory(this, argtype);
        parms.push_back(expr);
    }
}

void win_funcall::out(std::ostream &out)
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
            out << ",";
    }
    if (proc->ident() == "count" || proc->ident() == "COUNT")
        if (parms.begin() == parms.end())
            out << "*";

    out << ")";
}

void win_funcall::accept(prod_visitor *v)
{
    v->visit(this);
    for (auto p : parms)
        p->accept(v);
}

void win_funcall::equivalent_transform()
{
    value_expr::equivalent_transform();

    if (schema::target_dbms == "clickhouse")
        return;
    
    for (auto &parm : parms)
        parm->equivalent_transform();
}

void win_funcall::back_transform()
{
    value_expr::back_transform();
    
    if (schema::target_dbms == "clickhouse")
        return;
    
    for (auto &parm : parms)
        parm->back_transform();
}

void win_funcall::set_component_id(int &id)
{
    value_expr::set_component_id(id);
    for (auto &parm : parms)
        parm->set_component_id(id);
}

bool win_funcall::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    for (auto &parm : parms)
        GET_COMPONENT_FROM_ID_CHILD(id, component, parm);
    return value_expr::get_component_from_id(id, component);
}

bool win_funcall::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    for (auto &parm : parms)
        SET_COMPONENT_FROM_ID_CHILD(id, component, parm);
    return value_expr::set_component_from_id(id, component);
}