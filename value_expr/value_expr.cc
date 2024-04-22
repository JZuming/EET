#include "value_expr.hh"
#include "expr.hh"

extern int in_in_clause; // 0-> not in "in" clause, 1-> in "in" clause (cannot use limit)

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint,
                                           vector<shared_ptr<named_relation>> *prefer_refs)
{
    try
    {
        if (p->scope->schema->booltype == type_constraint)
            return bool_expr::factory(p);

        if (type_constraint == NULL && d12() == 1)
            return bool_expr::factory(p);

        if (p->level < d6())
        {
            auto choice = d42();
            if ((choice <= 2) && window_function::allowed(p))
                return make_shared<window_function>(p, type_constraint);
            if (choice == 3)
                return make_shared<coalesce>(p, type_constraint);
            if (choice == 4)
                return make_shared<nullif>(p, type_constraint);
            if (choice <= 11)
                return make_shared<funcall>(p, type_constraint);
            if (choice <= 16)
                return make_shared<case_expr>(p, type_constraint);
            if (choice <= 21)
                return make_shared<binop_expr>(p, type_constraint);
        }
        auto choice = d42();
        if (in_in_clause == 0 && choice <= 12)
            return make_shared<atomic_subselect>(p, type_constraint);
        if (p->scope->refs.size() && choice <= 40)
            return make_shared<column_reference>(p, type_constraint, prefer_refs);
        return make_shared<const_expr>(p, type_constraint);
    }
    catch (runtime_error &e)
    {
    }
    p->retry();
    return factory(p, type_constraint);
}

void value_expr::equivalent_transform()
{
    mark_transformed();
    if (eq_value_expr) // if it already has equal expression, just skip
        return;

    shared_ptr<value_expr> random_value;
    shared_ptr<bool_expr> random_bool;
    // int fail_time = 0;
    while (1)
    {
        try
        {
            random_value = value_expr::factory(this, type);
            random_bool = bool_expr::factory(this);
            break;
        }
        catch (exception &e)
        {
            // fail_time++;
            // cerr << fail_time << " exception in value_expr::equivalent_transform [" << e.what() << "]" << endl;
            continue;
        }
    }
    auto null_deleter = [](value_expr *) {};
    auto share_this = shared_ptr<value_expr>(this, null_deleter);

    auto not_rand = make_shared<not_expr>(this, random_bool);
    auto rand_is_null = make_shared<null_predicate>(this, random_bool, true);
    auto rand_is_not_null = make_shared<null_predicate>(this, random_bool, false);

    int choice = d9();
    if (choice <= 3)
    {   
        // case when true then this_expr else random_value end
        // true <=> (rand_bool) or (not rand_bool) or (rand_bool is null)
        auto rand_or_not_rand = make_shared<bool_term>(this, true, random_bool, not_rand);
        auto true_value = make_shared<bool_term>(this, true, rand_or_not_rand, rand_is_null);
        eq_value_expr = make_shared<case_expr>(this, true_value, share_this, random_value);
    }
    else if (choice <= 6)
    {   // case when false then random_value else this_expr end
        // false <=> (rand_bool) and (not rand_bool) and (rand_bool is not null)
        auto rand_and_not_rand = make_shared<bool_term>(this, false, random_bool, not_rand);
        auto false_value = make_shared<bool_term>(this, false, rand_and_not_rand, rand_is_not_null);
        eq_value_expr = make_shared<case_expr>(this, false_value, random_value, share_this);
    }
    else {
        // should unmark, otherwise in printed_expr, share_this will print *eq_value_expr, which has not been set
        unmark_transformed(); 
        
        // case when random_bool then non_eq_trans_expr else this_expr end, or
        // case when random_bool then this_expr else non_eq_trans_expr end
        auto non_eq_trans_expr = make_shared<printed_expr>(this, share_this);
        if (d6() >= 3)
            eq_value_expr = make_shared<case_expr>(this, random_bool, non_eq_trans_expr, share_this);
        else
            eq_value_expr = make_shared<case_expr>(this, random_bool, share_this, non_eq_trans_expr);
        
        mark_transformed();
    }
}

void value_expr::back_transform()
{
    unmark_transformed();
}

void value_expr::out_eq_value_expr(ostream &out)
{
    assert(has_print_eq_expr == false);
    has_print_eq_expr = true;
    out << *eq_value_expr;
    has_print_eq_expr = false;
}

void value_expr::mark_transformed()
{
    assert(is_transformed == false);
    is_transformed = true;
}

void value_expr::unmark_transformed()
{
    assert(is_transformed == true);
    is_transformed = false;
};

void value_expr::set_component_id(int &id)
{
    assert(is_transformed == false);
    component_id = id;
    id++;
};