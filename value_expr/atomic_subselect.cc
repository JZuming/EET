#include "atomic_subselect.hh"

atomic_subselect::atomic_subselect(prod *p, sqltype *type_constraint)
    : value_expr(p), offset((d6() == 6) ? d100() : d6())
{
    match();
    if (d6() < 3)
    {
        if (type_constraint)
        {
            auto idx = scope->schema->aggregates_returning_type;
            auto iters = idx.equal_range(type_constraint);
            agg = random_pick<>(iters)->second;
        }
        else
        {
            agg = &random_pick<>(scope->schema->aggregates);
        }
        if (agg->argtypes.size() != 1)
            agg = 0;
        else
            type_constraint = agg->argtypes[0];
    }
    else
    {
        agg = 0;
    }

    if (type_constraint)
    {
        auto idx = scope->schema->tables_with_columns_of_type;
        col = NULL;
        auto iters = idx.equal_range(type_constraint);
        tab = random_pick<>(iters)->second;

        for (auto &cand : tab->columns())
        {
            if (type_constraint->consistent(cand.type))
            {
                col = &cand;
                break;
            }
        }
        assert(col);
    }
    else
    {
        tab = random_pick<>(scope->tables);
        col = &random_pick<>(tab->columns());
    }

    type = agg ? agg->restype : col->type;
}

void atomic_subselect::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }

    out << "(select ";
    if (agg)
        out << agg->ident() << "(" << col->name << ")";
    else
        out << col->name;

    out << " from " << tab->ident();

    if (!agg)
        out << " order by " << col->name << " limit 1 offset " << offset;

    out << ")";
    indent(out);
}