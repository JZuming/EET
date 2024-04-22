#include "bool_expr.hh"
#include "../expr.hh"

shared_ptr<bool_expr> bool_expr::factory(prod *p)
{
    try
    {
        if (p->level > d20())
            return make_shared<const_bool>(p);

        auto choose = d42();
        if (choose <= 13)
            return make_shared<comparison_op>(p);
        else if (choose <= 26)
            return make_shared<bool_term>(p);
        else if (choose <= 28)
            return make_shared<not_expr>(p);
        else if (choose <= 30)
            return make_shared<null_predicate>(p);
        else if (choose <= 32)
            return make_shared<const_bool>(p);
        else if (choose <= 34)
            return make_shared<between_op>(p);
        else if (choose <= 36)
            return make_shared<like_op>(p);
        else if (choose <= 38)
            return make_shared<in_query>(p);
        else if (choose <= 40)
            return make_shared<comp_subquery>(p);
        else
            return make_shared<exists_predicate>(p);
        //     return make_shared<distinct_pred>(q);
    }
    catch (runtime_error &e)
    {
    }
    p->retry();
    return factory(p);
}

void bool_expr::equivalent_transform()
{
    mark_transformed();
    if (eq_bool_expr) // if it already has equal expression, just skip
        return;

    shared_ptr<bool_expr> random_bool;
    while (1)
    {
        try
        {
            random_bool = bool_expr::factory(this);
            break;
        }
        catch (exception &e)
        {
            continue;
        }
    }
    auto null_deleter = [](bool_expr *) {};
    auto share_this = shared_ptr<bool_expr>(this, null_deleter);

    auto not_rand = make_shared<not_expr>(this, random_bool);
    auto rand_is_null = make_shared<null_predicate>(this, random_bool, true);
    auto rand_is_not_null = make_shared<null_predicate>(this, random_bool, false);

    bool is_case_true = d6() > 3 ? true : false;
    if (is_case_true)
    {   
        // case when true then this_expr else random_value end
        // true <=> (rand_bool) or (not rand_bool) or (rand_bool is null)
        auto rand_or_not_rand = make_shared<bool_term>(this, true, random_bool, not_rand);
        auto truth_value = make_shared<bool_term>(this, true, rand_or_not_rand, rand_is_null);
        eq_bool_expr = make_shared<bool_term>(this, false, truth_value, share_this);
    }
    else
    {   
        // case when false then random_value else this_expr end
        // false <=> (rand_bool) and (not rand_bool) and (rand_bool is not null)
        auto rand_and_not_rand = make_shared<bool_term>(this, false, random_bool, not_rand);
        auto false_value = make_shared<bool_term>(this, false, rand_and_not_rand, rand_is_not_null);
        eq_bool_expr = make_shared<bool_term>(this, true, false_value, share_this);
    }
}

void bool_expr::back_transform()
{
    unmark_transformed();
}

void bool_expr::out_eq_bool_expr(ostream &out)
{
    assert(has_print_eq_expr == false);
    has_print_eq_expr = true;
    out << *eq_bool_expr;
    has_print_eq_expr = false;
}