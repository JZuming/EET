#include <typeinfo>
#include <numeric>
#include <algorithm>
#include <stdexcept>
#include <cassert>

#include "random.hh"
#include "relmodel.hh"
#include "grammar.hh"
#include "schema.hh"
#include "impedance.hh"

using namespace std;

int use_group = 2; // 0->no group, 1->use group, 2->to_be_define
int in_in_clause = 0; // 0-> not in "in" clause, 1-> in "in" clause
table *handle_table = NULL;

int write_op_id = 0; // start from 10000
static int row_id = 10000; // start from 10000

static set<string> created_col_names;
static set<string> created_win_names;
static set<string> exist_table_name;
static set<string> exist_index_name;
static bool table_init = false;
static bool index_init = false;

void clear_naming_data () {
    created_col_names.clear();
    created_win_names.clear();
    exist_table_name.clear();
    exist_index_name.clear();
    table_init = false;
    index_init = false;
}

static void exclude_tables(
    table *victim,
    vector<named_relation *> &target_tables,
    multimap<sqltype*, table*> &target_t_with_c_of_type,
    vector<named_relation *> &excluded_tables,
    vector<pair<sqltype*, table*>> &excluded_t_with_c_of_type)
{
    for (std::size_t i = 0; i < target_tables.size(); i++) {
        if (target_tables[i]->ident() == victim->ident()) {
            excluded_tables.push_back(target_tables[i]);
            target_tables.erase(target_tables.begin() + i);
            i--;
        }
    }

    for (auto iter = target_t_with_c_of_type.begin(); iter != target_t_with_c_of_type.end();) {
        if (iter->second->ident() == victim->ident()) {
            excluded_t_with_c_of_type.push_back(*iter);
            iter = target_t_with_c_of_type.erase(iter);
        }
        else
            iter++;
    }
}

static void recover_tables(
    vector<named_relation *> &target_tables,
    multimap<sqltype*, table*> &target_t_with_c_of_type,
    vector<named_relation *> &excluded_tables,
    vector<pair<sqltype*, table*>> &excluded_t_with_c_of_type)
{
    for (std::size_t i = 0; i < excluded_tables.size(); i++) {
        target_tables.push_back(excluded_tables[i]);
    }
    for (auto iter = excluded_t_with_c_of_type.begin(); iter != excluded_t_with_c_of_type.end(); iter++) {
        target_t_with_c_of_type.insert(*iter);
    }
}

shared_ptr<table_ref> table_ref::factory(prod *p, bool no_join) {
    try {
        if (p->level < 3 + d6()) {
            if (d6() > 3 && p->level < d6())
	            return make_shared<table_subquery>(p);
            if (no_join == false && d6() > 3)
	            return make_shared<joined_table>(p);
        }
        return make_shared<table_or_query_name>(p);
    } catch (runtime_error &e) {
        p->retry();
    }
    return factory(p, no_join);
}

table_or_query_name::table_or_query_name(prod *p, bool only_base_table) : table_ref(p) {
    if (only_base_table == false)
        t = random_pick(scope->tables);
    else {
        vector<table *> base_tables;
        for (auto& name_ptr:scope->tables) {
            auto table_ptr = dynamic_cast<table *>(name_ptr);
            if (table_ptr && table_ptr->is_base_table == true) 
                base_tables.push_back(table_ptr);
        }
        t = random_pick(base_tables);
    }
    refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("ref"), t));
}

table_or_query_name::table_or_query_name(prod *p, table *target_table) : table_ref(p) {
    t = target_table;
    refs.push_back(make_shared<aliased_relation>(t->ident(), t));
}

void table_or_query_name::out(std::ostream &out) {
    if (refs[0]->ident() != t->ident())
        out << t->ident() << " as " << refs[0]->ident();
    else
        out << t->ident();
}

target_table::target_table(prod *p, table *victim) : table_ref(p)
{
  while (! victim
	 || victim->schema == "pg_catalog"
	 || !victim->is_base_table
	 || !victim->columns().size()) {
    struct named_relation *pick = random_pick(scope->tables);
    victim = dynamic_cast<table *>(pick);
    retry();
  }
  victim_ = victim;
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("target"), victim));
}

void target_table::out(std::ostream &out) {
  out << victim_->ident() << " as " << refs[0]->ident();
}

table_sample::table_sample(prod *p) : table_ref(p) {
  match();
  retry_limit = 1000; /* retries are cheap here */
  do {
    auto pick = random_pick(scope->schema->base_tables);
    t = dynamic_cast<struct table*>(pick);
    retry();
  } while (!t || !t->is_base_table);
  
  refs.push_back(make_shared<aliased_relation>(scope->stmt_uid("sample"), t));
  percent = 0.1 * d100();
  method = (d6() > 2) ? "system" : "bernoulli";
}

void table_sample::out(std::ostream &out) {
  out << t->ident() <<
    " as " << refs[0]->ident() <<
    " tablesample " << method <<
    " (" << percent << ") ";
}

table_subquery::table_subquery(prod *p, bool lateral)
  : table_ref(p), is_lateral(lateral) {
    query = make_shared<query_spec>(this, scope, lateral);
    string alias = scope->stmt_uid("subq");
    relation *aliased_rel = &query->select_list->derived_table;
    refs.push_back(make_shared<aliased_relation>(alias, aliased_rel));
}

table_subquery::~table_subquery() { }

void table_subquery::accept(prod_visitor *v) {
    query->accept(v);
    v->visit(this);
}

shared_ptr<join_cond> join_cond::factory(prod *p, table_ref &lhs, table_ref &rhs)
{
    try {
        if (schema::target_dbms == "tidb" || schema::target_dbms == "clickhouse"
            || schema::target_dbms == "postgres")
            return make_shared<simple_join_cond>(p, lhs, rhs);
        
        if (d6() >= 4)
            return make_shared<expr_join_cond>(p, lhs, rhs);
        else
            return make_shared<simple_join_cond>(p, lhs, rhs);
    } catch (runtime_error &e) {
        p->retry();
    }
    return factory(p, lhs, rhs);
}

simple_join_cond::simple_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs)
{
retry:
  named_relation *left_rel = &*random_pick(lhs.refs);
  
  if (!left_rel->columns().size())
    { retry(); goto retry; }

  named_relation *right_rel = &*random_pick(rhs.refs);

  column &c1 = random_pick(left_rel->columns());

  for (auto c2 : right_rel->columns()) {
    if (c1.type == c2.type) {
      condition +=
	left_rel->ident() + "." + c1.name + " = " + right_rel->ident() + "." + c2.name + " ";
      break;
    }
  }
  if (condition == "") {
    retry(); goto retry;
  }
}

void simple_join_cond::out(std::ostream &out) {
     out << condition;
}

expr_join_cond::expr_join_cond(prod *p, table_ref &lhs, table_ref &rhs)
     : join_cond(p, lhs, rhs), joinscope(p->scope)
{
    joinscope.refs.clear(); // only use the refs in lhs and rhs
    scope = &joinscope;
    for (auto ref: lhs.refs)
        joinscope.refs.push_back(&*ref);
    for (auto ref: rhs.refs)
        joinscope.refs.push_back(&*ref);
    search = bool_expr::factory(this);
}

void expr_join_cond::out(std::ostream &out) {
     out << *search;
}

joined_table::joined_table(prod *p) : table_ref(p) {
    lhs = table_ref::factory(this);
    if (schema::target_dbms == "clickhouse") {
        rhs = table_ref::factory(this, true);
        assert(!dynamic_pointer_cast<joined_table>(rhs));
    }
    else {
        rhs = table_ref::factory(this);
    }

    assert(!scope->schema->supported_join_op.empty());
    type = random_pick(scope->schema->supported_join_op);
    
    auto tmp_group = use_group;
    use_group = 0;
    if (type != "cross")
        condition = join_cond::factory(this, *lhs, *rhs);
    use_group = tmp_group;

    for (auto ref: lhs->refs)
        refs.push_back(ref);
    for (auto ref: rhs->refs)
        refs.push_back(ref);
}

void joined_table::out(std::ostream &out) {
    if (scope->schema->target_dbms != "clickhouse")
        out << "(";
    
    out << *lhs;
    indent(out);
    out << type << " join " << *rhs;
    indent(out);
    if (type != "cross")
        out << "on (" << *condition << ")";
    
    if (scope->schema->target_dbms != "clickhouse")
        out << ")";
}

void table_subquery::out(std::ostream &out) {
    if (is_lateral)
        out << "lateral ";
    out << "(" << *query << ") as " << refs[0]->ident();
}

void from_clause::out(std::ostream &out) {
    if (! reflist.size())
        return;
    out << "from ";

    for (auto r = reflist.begin(); r < reflist.end(); r++) {
        indent(out);
        out << **r;
        if (r + 1 != reflist.end())
            out << ",";
    }
}

from_clause::from_clause(prod *p, bool only_base_table) : prod(p) {
    if (only_base_table)
        reflist.push_back(make_shared<table_or_query_name>(this, true));
    else
        reflist.push_back(table_ref::factory(this));
    for (auto r : reflist.back()->refs)
        scope->refs.push_back(&*r);
}

from_clause::from_clause(prod *p, table *from_table) : prod(p) {
    reflist.push_back(make_shared<table_or_query_name>(this, from_table));
    for (auto r : reflist.back()->refs)
        scope->refs.push_back(&*r);
}

select_list::select_list(prod *p, 
                    vector<shared_ptr<named_relation> > *refs, 
                    vector<sqltype *> *pointed_type,
                    bool select_all) :
 prod(p), prefer_refs(refs)
{
    if (select_all && pointed_type != NULL) 
        throw std::runtime_error("select_all and pointed_type cannot be used at the same time");
    
    if (select_all && refs == NULL)
        throw std::runtime_error("select_all and refs should be used at the same time");
    
    if (select_all) { // pointed_type is null and prefer_refs is not null
        // auto r = &*random_pick(*prefer_refs);
        for (auto& r : *prefer_refs) {
            for (auto& col : r->columns()) {
                auto expr = make_shared<column_reference>(this, (sqltype *)0, prefer_refs);
                expr->type = col.type;
                expr->reference = r->ident() + "." + col.name;
                expr->table_ref = r->ident();
                
                value_exprs.push_back(expr);
                ostringstream name;
                if (schema::target_dbms == "clickhouse")
                    name << "c_" << level << "_" << unique_column_name() << "_" << columns++; // for unique columns name
                else
                    name << "c_" << columns++;
                sqltype *t = expr->type;
                derived_table.columns().push_back(column(name.str(), t));
            }
        }
        return;
    }

    if (pointed_type == NULL) {
        do {
            shared_ptr<value_expr> e;
            if (schema::target_dbms == "clickhouse")
                e = make_shared<column_reference>(this, (sqltype *)0, prefer_refs);
            else
                e = value_expr::factory(this, (sqltype *)0, prefer_refs);
            value_exprs.push_back(e);
            ostringstream name;
            if (schema::target_dbms == "clickhouse")
                name << "c_" << level << "_" << unique_column_name() << "_" << columns++; // for unique columns name
            else
                name << "c_" << columns++;
            sqltype *t = e->type;
            assert(t);
            derived_table.columns().push_back(column(name.str(), t));
        } while (d6() > 1);

        return;
    }

    // pointed_type is not null
    for (size_t i = 0; i < pointed_type->size(); i++) {
        shared_ptr<value_expr> e;
        if (schema::target_dbms == "clickhouse")
            e = make_shared<column_reference>(this, (*pointed_type)[i], prefer_refs);
        else
            e = value_expr::factory(this, (*pointed_type)[i], prefer_refs);
        value_exprs.push_back(e);
        ostringstream name;
        if (schema::target_dbms == "clickhouse")
            name << "c_" << level << "_" << unique_column_name() << "_" << columns++; // for unique columns name
        else
            name << "c_" << columns++;
        sqltype *t = e->type;
        assert(t);
        derived_table.columns().push_back(column(name.str(), t));
    }
}

void select_list::out(std::ostream &out)
{
    int i = 0;
    for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++) {
        indent(out);
        out << **expr << " as " << derived_table.columns()[i].name;
        i++;
        if (expr+1 != value_exprs.end())
            out << ", ";
    }
}

void query_spec::out(std::ostream &out) {
    out << "select " << set_quantifier << " " << *select_list;
    indent(out);
    out << *from_clause;
    indent(out);
    out << "where " << *search;

    if (has_group) {
        indent(out);
        out << *group_clause;
    }

    if (has_window) {
        indent(out);
        out << *window_clause;
    }

    if (has_order) {
        indent(out);
        out << "order by ";
        for (auto ref = order_clause.begin(); ref != order_clause.end(); ref++) {
            auto& order_pair = *ref;
            out << order_pair.first << " ";
            out << (order_pair.second ? "asc" : "desc");
            if (ref + 1 != order_clause.end())
                out << ", ";
        }
    }

    if (has_limit) {
        indent(out);
        out << " limit " << limit_num;
    }
}

struct for_update_verify : prod_visitor {
  virtual void visit(prod *p) {
    if (dynamic_cast<window_function*>(p))
      throw("window function");
    joined_table* join = dynamic_cast<joined_table*>(p);
    if (join && join->type != "inner")
      throw("outer join");
    query_spec* subquery = dynamic_cast<query_spec*>(p);
    if (subquery)
      subquery->set_quantifier = "";
    table_or_query_name* tab = dynamic_cast<table_or_query_name*>(p);
    if (tab) {
      table *actual_table = dynamic_cast<table*>(tab->t);
      if (actual_table && !actual_table->is_insertable)
	throw("read only");
      if (actual_table->name.find("pg_"))
	throw("catalog");
    }
    table_sample* sample = dynamic_cast<table_sample*>(p);
    if (sample) {
      table *actual_table = dynamic_cast<table*>(sample->t);
      if (actual_table && !actual_table->is_insertable)
	throw("read only");
      if (actual_table->name.find("pg_"))
	throw("catalog");
    }
  } ;
};


select_for_update::select_for_update(prod *p, struct scope *s, bool lateral)
  : query_spec(p,s,lateral)
{
  static const char *modes[] = {
    "update",
    "share",
    "no key update",
    "key share",
  };

  try {
    for_update_verify v1;
    this->accept(&v1);

  } catch (const char* reason) {
    lockmode = 0;
    return;
  }
  lockmode = modes[d6()%(sizeof(modes)/sizeof(*modes))];
  set_quantifier = ""; // disallow distinct
}

void select_for_update::out(std::ostream &out) {
  query_spec::out(out);
  if (lockmode) {
    indent(out);
    out << " for " << lockmode;
  }
}

query_spec::query_spec(prod *p, struct scope *s, 
                    bool lateral, vector<sqltype *> *pointed_type,
                    bool txn_mode) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;

    if (txn_mode)
        use_group = 0;

    // group will change the type of columns in select_list, so donnot use group when need pointed_type
    if (pointed_type != NULL)
        use_group = 0;

    if (use_group == 2) { // confirm whether use group
        if (d6() == 1) 
            use_group = 1;
        else 
            use_group = 0;
    }

    if (lateral)
        scope->refs = s->refs;
    
    int tmp_group = use_group; // store use_group temporarily
    
    // from clause can use "group by".
    use_group = 2; 
    from_clause = make_shared<struct from_clause>(this, txn_mode); // txn testing: need to know which rows are read, so just from a table

    use_group = 0; // cannot use "group by" in "where" and "select" clause.
    search = bool_expr::factory(this); 
    
    // txn testing: need to know all info of columns so select * from table_name where
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, pointed_type, txn_mode);
    
    use_group = tmp_group; // recover use_group

    if (use_group == 1) {
        group_clause = make_shared<struct group_clause>(this, this->scope, select_list, &from_clause->reflist.back()->refs);
        has_group = true;
    }

    int only_allow_level_0 = false;
#ifdef TEST_MONETDB
    only_allow_level_0 = true;
#endif

    if (only_allow_level_0 && this->level != 0)
        return;

    if (!txn_mode && !has_group && d9() == 1) {
        has_window = true;
        window_clause = make_shared<named_window>(this, this->scope);
        auto &select_exprs = select_list->value_exprs;
        auto size = select_exprs.size();

        for (size_t i = 0; i < size; i++) {
            if (d6() > 3) // 50%
                continue;
            auto new_expr = make_shared<win_func_using_exist_win>(this, select_exprs[i]->type, window_clause->window_name);
            select_exprs.erase(select_exprs.begin() + i);
            select_exprs.insert(select_exprs.begin() + i, new_expr);
            select_list->derived_table.columns()[i].type = new_expr->type;
        }
    }

    if (has_group == false && d6() == 1) {
        has_order = true;
        auto &selected_columns = select_list->derived_table.columns();
        for (auto& col : selected_columns) {
            auto col_name = col.name;
            auto is_asc = d6() > 3 ? true : false;
            order_clause.push_back(make_pair<>(col_name, is_asc));
        }
    }

    // "txn_mode" and "qcn_select_mode" need to get all row, cannot use limit
    // the subquery in clause cannot use limit (mysql) 
    // must used with "order by", otherwise the result is not determined
    if (!txn_mode && in_in_clause == 0 && has_order && d6() < 3) { 
        has_limit = true;
        limit_num = d100() + d100();
    }
    if (txn_mode || (schema::target_dbms == "clickhouse" && has_window))
        set_quantifier = "";
    else
        set_quantifier = (d9() == 1) ? "distinct" : "";
}

query_spec::query_spec(prod *p, struct scope *s,
              table *from_table, 
              shared_ptr<bool_expr> where_search) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    has_group = false;
    has_window = false;
    has_order = false;
    has_limit = false;
    use_group = 0;

    from_clause = make_shared<struct from_clause>(this, from_table);
    search = where_search; 
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, (vector<sqltype *> *)NULL, true);
}

query_spec::query_spec(prod *p, struct scope *s,
              table *from_table, 
              op *target_op, 
              shared_ptr<value_expr> left_operand,
              shared_ptr<value_expr> right_operand) :
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    has_group = false;
    has_window = false;
    has_order = false;
    has_limit = false;
    use_group = 0;

    from_clause = make_shared<struct from_clause>(this, from_table);
    search = make_shared<struct comparison_op>(this, target_op, left_operand, right_operand);
    select_list = make_shared<struct select_list>(this, &from_clause->reflist.back()->refs, (vector<sqltype *> *)NULL, true);
}

long prepare_stmt::seq;

void modifying_stmt::pick_victim()
{
  do {
      struct named_relation *pick = random_pick(scope->tables);
      victim = dynamic_cast<struct table*>(pick);
      retry();
    } while (! victim
	   || victim->schema == "pg_catalog"
	   || !victim->is_base_table
	   || !victim->columns().size());
}

modifying_stmt::modifying_stmt(prod *p, struct scope *s, table *v)
  : prod(p), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    if (!v)
        pick_victim();
    else
        victim = v;
}

delete_stmt::delete_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p,s,v) {
    scope->refs.push_back(victim);
    write_op_id++;

    if (schema::target_dbms == "clickhouse")
        handle_table = victim;
    
    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);

    // can only select base table, because views may include the data of target table
    vector<table*> views;
    for (auto t : scope->tables) {
        auto real_t = dynamic_cast<table*>(t);
        if (real_t && real_t->is_base_table == false)
            views.push_back(real_t);
    }
    for (auto v : views) {
        exclude_tables(v, scope->tables, scope->schema->tables_with_columns_of_type, 
                        excluded_tables, excluded_t_with_c_of_type);
    }

    search = bool_expr::factory(this);

    // Do not recover, because equivalent transform may also use the scope 
    // And recovering is unnecessary.
    // recover_tables(scope->tables, 
    //                 scope->schema->tables_with_columns_of_type, 
    //                 excluded_tables, 
    //                 excluded_t_with_c_of_type);
}

void delete_stmt::out(ostream &out) {
    if (schema::target_dbms == "clickhouse") 
        out << "alter table " << victim->ident() << " delete";
    else 
        out << "delete from " << victim->ident();
    indent(out);
    out << "where " << std::endl << *search;
}

delete_returning::delete_returning(prod *p, struct scope *s, table *victim)
  : delete_stmt(p, s, victim) {
  match();
  select_list = make_shared<struct select_list>(this);
}

insert_stmt::insert_stmt(prod *p, struct scope *s, table *v, bool only_const)
  : modifying_stmt(p, s, v)
{
    match();
    write_op_id++;

    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
    
    // can only select base table, because views may include the data of target table
    vector<table*> views;
    for (auto t : scope->tables) {
        auto real_t = dynamic_cast<table*>(t);
        if (real_t && real_t->is_base_table == false)
            views.push_back(real_t);
    }
    for (auto v : views) {
        exclude_tables(v, scope->tables, scope->schema->tables_with_columns_of_type, 
                        excluded_tables, excluded_t_with_c_of_type);
    }
    
    auto insert_num = 1;
    // select valued columns (all)
    for (auto& col : victim->columns()) {
        // if (col.name == PKEY_IDENT || col.name == VKEY_IDENT || d6() > 1) 
        if (schema::target_dbms == "sqlite" && col.name == "rowid")
            continue;
        valued_column_name.push_back(col.name);
    }

    // should contain at least one column
    if (valued_column_name.empty()) {
        auto col = random_pick(victim->columns());
        valued_column_name.push_back(col.name);
    }
    
    for (auto i = 0; i < insert_num; i++) {
        vector<shared_ptr<value_expr> > value_exprs;
        for (auto col : victim->columns()) {
            if (find(valued_column_name.begin(), valued_column_name.end(), col.name) == valued_column_name.end())
                continue;
            
            if (col.name == VKEY_IDENT) {
                auto expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(write_op_id); // use write_op_id
                value_exprs.push_back(expr);
                continue;
            }

            if (col.name == PKEY_IDENT) {
                row_id += 1000;
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(row_id); // use write_op_id
                value_exprs.push_back(expr);
                continue;
            }

            if (only_const) {
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                value_exprs.push_back(expr);
            }
            else {
                auto expr = value_expr::factory(this, col.type);
                assert(expr->type == col.type);
                value_exprs.push_back(expr);
            }
        }
        value_exprs_vector.push_back(value_exprs);
    }

    // Do not recover, because equivalent transform may also use the scope 
    // And recovering is unnecessary.
    // recover_tables(scope->tables, 
    //                 scope->schema->tables_with_columns_of_type, 
    //                 excluded_tables, 
    //                 excluded_t_with_c_of_type);
}

void insert_stmt::out(std::ostream &out)
{
    out << "insert into " << victim->ident() << " (";
    auto col_num = valued_column_name.size(); 
    for (int i = 0; i < col_num; i++) {
        out << valued_column_name[i];
        if (i + 1 < col_num)
            out << ", ";
    }
    out << ") ";

    if (!value_exprs_vector.size()) {
        out << "default values";
        return;
    }

    out << "values ";
    
    for (auto value_exprs = value_exprs_vector.begin(); value_exprs != value_exprs_vector.end(); value_exprs++) {
        indent(out);
        out << "(";
        for (auto expr = value_exprs->begin();
          expr != value_exprs->end(); expr++) {
            out << **expr;
            if (expr + 1 != value_exprs->end())
                out << ", ";
        }
        out << ")";
        if (value_exprs + 1 != value_exprs_vector.end()) 
            out << ", ";
    }
}

set_list::set_list(prod *p, table *target) : prod(p), myscope(p->scope)
{
    // update's scope might change (e.g. add victim table to scope->refs), 
    // use the current version only
    scope = &myscope;
    
    do {
        for (auto col : target->columns()) {
            if (name_set.count(col.name) != 0)
                continue;
            
            if (col.name == VKEY_IDENT) {
                auto  expr = make_shared<const_expr>(this, col.type);
                assert(expr->type == col.type);
                expr->expr = to_string(write_op_id); // use write_op_id
                
                value_exprs.push_back(expr);
                names.push_back(col.name);
                name_set.insert(col.name);
                continue;
            }

            if (col.name == PKEY_IDENT)
                continue;
            
            if (schema::target_dbms == "sqlite" && col.name == "rowid")
                continue;
            
            if (d6() < 4)
	            continue;
            
            auto expr = value_expr::factory(this, col.type);
            value_exprs.push_back(expr);
            names.push_back(col.name);
            name_set.insert(col.name);
        }
    } while (names.size() < 2);
}

void set_list::out(std::ostream &out)
{
    assert(names.size());
    if (schema::target_dbms == "clickhouse")
        out << " update ";
    else
        out << " set ";
    
    for (size_t i = 0; i < names.size(); i++) {
        indent(out);
        out << names[i] << " = " << *value_exprs[i];
        if (i + 1 != names.size())
            out << ", ";
    }
}

update_stmt::update_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v) {
    
    write_op_id++;

    if (schema::target_dbms == "clickhouse")
        handle_table = victim;
    
    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
    
    // can only select base table, because views may include the data of target table
    vector<table*> views;
    for (auto t : scope->tables) {
        auto real_t = dynamic_cast<table*>(t);
        if (real_t && real_t->is_base_table == false)
            views.push_back(real_t);
    }
    for (auto v : views) {
        exclude_tables(v, scope->tables, scope->schema->tables_with_columns_of_type, 
                        excluded_tables, excluded_t_with_c_of_type);
    }

    // IMPORTANT: expr in set_list cannot reference the columns in target table
    // We have to add victim table to 'scope->refs' after construct 'set_list'
    set_list = make_shared<struct set_list>(this, victim);
    scope->refs.push_back(victim);
    search = bool_expr::factory(this);
    
    // Do not recover, because equivalent transform may also use the scope 
    // And recovering is unnecessary.
    // // recover the scope->tables, scope->schema->tables_with_columns_of_type
    // recover_tables(scope->tables, 
    //                 scope->schema->tables_with_columns_of_type, 
    //                 excluded_tables, 
    //                 excluded_t_with_c_of_type);
}

void update_stmt::out(std::ostream &out)
{
    if (schema::target_dbms == "clickhouse") {
        out << "alter table ";
    } else {
        out << "update ";
    }
    out << victim->ident() << *set_list;
    indent(out);
    out << "where " << *search;
}

update_returning::update_returning(prod *p, struct scope *s, table *v)
  : update_stmt(p, s, v) {
  match();

  select_list = make_shared<struct select_list>(this);
}


upsert_stmt::upsert_stmt(prod *p, struct scope *s, table *v)
  : insert_stmt(p,s,v)
{
  match();

  if (!victim->constraints.size())
    fail("need table w/ constraint for upsert");
    
  set_list = std::make_shared<struct set_list>(this, victim);
  search = bool_expr::factory(this);
  constraint = random_pick(victim->constraints);
}

void common_table_expression::accept(prod_visitor *v)
{
  v->visit(this);
  for(auto q : with_queries)
    q->accept(v);
  query->accept(v);
}

common_table_expression::common_table_expression(prod *parent, struct scope *s, bool txn_mode)
  : prod(parent), myscope(s)
{
    scope = &myscope;
    do {
        shared_ptr<query_spec> query = make_shared<query_spec>(this, s, false, (vector<sqltype *> *)NULL, txn_mode);
        with_queries.push_back(query);
        string alias = scope->stmt_uid("cte");
        relation *relation = &query->select_list->derived_table;
        auto aliased_rel = make_shared<aliased_relation>(alias, relation);
        refs.push_back(aliased_rel);
        scope->tables.push_back(&*aliased_rel);

    } while (d6() > 2);

retry:
    try {
        query = make_shared<query_spec>(this, scope, false, (vector<sqltype *> *)NULL, txn_mode);
    } catch (runtime_error &e) {
        retry();
        goto retry;
    }

}

void common_table_expression::out(std::ostream &out)
{
    out << "WITH " ;
    for (size_t i = 0; i < with_queries.size(); i++) {
        indent(out);
        out << refs[i]->ident() << " AS " << "(" << *with_queries[i] << ")";
        if (i+1 != with_queries.size())
            out << ", ";
        indent(out);
    }
    out << *query;
    indent(out);
}

merge_stmt::merge_stmt(prod *p, struct scope *s, table *v)
     : modifying_stmt(p,s,v) {
  match();
  target_table_ = make_shared<target_table>(this, victim);
  data_source = table_ref::factory(this);
//   join_condition = join_cond::factory(this, *target_table_, *data_source);
  join_condition = make_shared<simple_join_cond>(this, *target_table_, *data_source);


  /* Put data_source into scope but not target_table.  Visibility of
     the latter varies depending on kind of when clause. */
//   for (auto r : data_source->refs)
//     scope->refs.push_back(&*r);

  clauselist.push_back(when_clause::factory(this));
  while (d6()>4)
    clauselist.push_back(when_clause::factory(this));
}

void merge_stmt::out(std::ostream &out)
{
     out << "MERGE INTO " << *target_table_;
     indent(out);
     out << "USING " << *data_source;
     indent(out);
     out << "ON " << *join_condition;
     indent(out);
     for (auto p : clauselist) {
       out << *p;
       indent(out);
     }
}

void merge_stmt::accept(prod_visitor *v)
{
  v->visit(this);
  target_table_->accept(v);
  data_source->accept(v);
  join_condition->accept(v);
  for (auto p : clauselist)
    p->accept(v);
    
}

when_clause::when_clause(merge_stmt *p)
  : prod(p)
{
  condition = bool_expr::factory(this);
  matched = d6() > 3;
}

void when_clause::out(std::ostream &out)
{
  out << (matched ? "WHEN MATCHED " : "WHEN NOT MATCHED");
  indent(out);
  out << "AND " << *condition;
  indent(out);
  out << " THEN ";
  out << (matched ? "DELETE" : "DO NOTHING");
}

void when_clause::accept(prod_visitor *v)
{
  v->visit(this);
  condition->accept(v);
}

when_clause_update::when_clause_update(merge_stmt *p)
  : when_clause(p), myscope(p->scope)
{
  myscope.tables = scope->tables;
  myscope.refs = scope->refs;
  scope = &myscope;
  scope->refs.push_back(&*(p->target_table_->refs[0]));
  
  set_list = std::make_shared<struct set_list>(this, p->victim);
}

void when_clause_update::out(std::ostream &out) {
  out << "WHEN MATCHED AND " << *condition;
  indent(out);
  out << " THEN UPDATE " << *set_list;
}

void when_clause_update::accept(prod_visitor *v)
{
  v->visit(this);
  set_list->accept(v);
}


when_clause_insert::when_clause_insert(struct merge_stmt *p)
  : when_clause(p)
{
  for (auto col : p->victim->columns()) {
    auto expr = value_expr::factory(this, col.type);
    assert(expr->type == col.type);
    exprs.push_back(expr);
  }
}

void when_clause_insert::out(std::ostream &out) {
  out << "WHEN NOT MATCHED AND " << *condition;
  indent(out);
  out << " THEN INSERT VALUES ( ";

  for (auto expr = exprs.begin();
       expr != exprs.end();
       expr++) {
    out << **expr;
    if (expr+1 != exprs.end())
      out << ", ";
  }
  out << ")";

}

void when_clause_insert::accept(prod_visitor *v)
{
  v->visit(this);
  for (auto p : exprs)
    p->accept(v);
}

shared_ptr<when_clause> when_clause::factory(struct merge_stmt *p)
{
  try {
    switch(d6()) {
    case 1:
    case 2:
      return make_shared<when_clause_insert>(p);
    case 3:
    case 4:
      return make_shared<when_clause_update>(p);
    default:
      return make_shared<when_clause>(p);
    }
  } catch (runtime_error &e) {
    p->retry();
  }
  return factory(p);
}

string upper_translate(string str)
{
    string ret;
    for (std::size_t i = 0; i < str.length(); i++) {
        if(str[i] >= 'a' && str[i] <= 'z') {
            ret.push_back(str[i] + 'A' - 'a');
            continue;
        }
        
        ret.push_back(str[i]);
    }
    return ret;
}

string unique_column_name()
{
    string column_name = "";
    string upper_case = "";
    int i = 0;
    while (1) {
        column_name = "c" + to_string(i);
        upper_case = upper_translate(column_name);
        if (created_col_names.count(upper_case) == 0) 
            break;
        i++;
    }
    created_col_names.insert(upper_case);
    return column_name;
}

string unique_window_name(void)
{
    string window_name = "";
    int i = 0;
    while (1) {
        window_name = "w" + to_string(i);
        if (created_win_names.count(upper_translate(window_name)) == 0) 
            break;
        i++;
    }
    created_win_names.insert(window_name);
    return window_name;
}

string unique_table_name(scope *s)
{
    if (table_init == false) {
        for (auto t : s->tables) 
            exist_table_name.insert(upper_translate(t->ident()));
        table_init = true;
    }
    
    string new_table_name;
    string upper_case;
    int i = 0;
    while (1) {
        new_table_name = "t" + to_string(i);
        upper_case = upper_translate(new_table_name);
        if (exist_table_name.count(upper_case) == 0) 
            break;
        i++;
    }
    exist_table_name.insert(upper_case);
    return new_table_name;
}

string unique_index_name(scope *s)
{
    if (index_init == false) {
        for (auto i : s->indexes) 
            exist_index_name.insert(upper_translate(i));
        index_init = true;
    }

    string new_index_name;
    string upper_case;
    int i = 0;
    while (1) {
        new_index_name = "i" + to_string(i);
        upper_case = upper_translate(new_index_name);
        if (exist_index_name.count(upper_case) == 0) 
            break;
        i++;
    }
    exist_index_name.insert(upper_case);
    return new_index_name;
}

// must partition primary key
string cockroach_table_option(prod* p, shared_ptr<table> created_table, int primary_col_id)
{
    auto partion_col = created_table->columns()[primary_col_id];
    
    string table_option = "partition ";
    table_option += "by ";

    auto rand = d9();

    // do not partition date
    if (partion_col.type == p->scope->schema->datetype || rand == 1) {
        table_option += "nothing";
        return table_option;
    }

    if (rand <= 5 || 
            (partion_col.type != p->scope->schema->inttype && partion_col.type != p->scope->schema->realtype)
        ) { // list partion
        table_option += "list (";
        table_option += partion_col.name + ") (\n";
        
        auto partition_num = dx(4);
        set<string> used_value;
        for (int i = 0; i < partition_num; i++) {

            table_option = table_option + "partition " + "p" + to_string(i) + " values in (";
            auto list_len = dx(4);
            for (int j = 0; j < list_len; j++) {
                string list_value;
                do {
                    auto rand_value = make_shared<const_expr>(p, partion_col.type);
                    ostringstream stmt_stream;
                    rand_value->out(stmt_stream);
                    list_value = stmt_stream.str();
                } while (used_value.count(list_value) > 0);

                table_option += list_value;
                used_value.insert(list_value);
                if (list_value == "0" || list_value == "-0" || list_value == "0.0" || list_value == "-0.0") {
                    used_value.insert("0");
                    used_value.insert("-0");
                    used_value.insert("0.0");
                    used_value.insert("-0.0");
                }

                if (j < list_len - 1)
                    table_option += ", ";
            }
            table_option += ")";

            if (i < partition_num - 1)
                table_option += ", \n";
        }
        table_option += ")";
        return table_option;
    }

    // the type should be int or real
    table_option += "range (";
    table_option += partion_col.name + ") (\n";
    auto partition_num = dx(4);

    int start_point = 0;
    int random_len = 100 / partition_num;
    for (int i = 0; i < partition_num; i++) {
        table_option = table_option + "partition " + "p" + to_string(i) + " values from (";

        auto rand_value_1 = start_point + dx(random_len);
        table_option += to_string(rand_value_1);
        table_option += ") to (";

        auto rand_value_2 = rand_value_1 + dx(random_len);
        table_option += to_string(rand_value_2);

        table_option += ")";

        start_point = rand_value_2;

        if (i < partition_num - 1)
            table_option += ", \n";
    }
    table_option += ")";
    return table_option;
}

string yugabyte_table_option(prod* p, shared_ptr<table> created_table)
{
    string table_option = "split ";
    // if (d6() <= 3) {
        table_option += "into ";
        auto tablet_num = d100();
        table_option += to_string(tablet_num) + " tablets";
        return table_option;
    // }

    // // not yet supported for hash partitioned tables
    // table_option += "at values (";
    // auto split_points = d9();
    // cerr << "column num: " << created_table->columns().size() << endl;
    // cerr << "split num: " << split_points << endl;
    // for (int i = 0; i < split_points; i++) {
    //     table_option += "(";
    //     auto ref_col_num = dx(created_table->columns().size());

    //     cerr << "ref_col_num: " << ref_col_num << endl;

    //     for (int j = 0; j < ref_col_num; j++) {
    //         auto rand_value = value_expr::factory(p, created_table->columns()[j].type);
    //         ostringstream stmt_stream;
    //         rand_value->out(stmt_stream);
    //         table_option += stmt_stream.str();

    //         if (j < ref_col_num - 1)
    //             table_option += ", ";
    //     }
    //     table_option += ")";
    //     if (i < split_points - 1)
    //         table_option += ", ";
    // }
    // table_option += ")";
    // return table_option;
}

create_table_stmt::create_table_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    // create table
    string table_name;
    table_name = unique_table_name(scope);
    created_table = make_shared<struct table>(table_name, "main", true, true);

    // create at least one integer column, which make index has at least one choice 
    // generate vkey (identify different versions)
    column wkey(VKEY_IDENT, scope->schema->inttype);
    constraints.push_back(""); // no constraint
    created_table->columns().push_back(wkey);
    
    // generate pkey (identify different rows)
    column pkey(PKEY_IDENT, scope->schema->inttype);
    constraints.push_back("");
    created_table->columns().push_back(pkey);

    // generate other columns
    int column_num = d9();
    for (int i = 0; i < column_num; i++) {
        string column_name = unique_column_name();
        vector<sqltype *> enable_type;
        enable_type.push_back(scope->schema->inttype);
        enable_type.push_back(scope->schema->texttype);
        enable_type.push_back(scope->schema->realtype);
        enable_type.push_back(scope->schema->datetype);
        auto type = random_pick<>(enable_type);

        column create_column(column_name, type);
        created_table->columns().push_back(create_column);
        string constraint_str = "";
        if (type == scope->schema->texttype && 
            !scope->schema->available_collation.empty() &&
            d6() <= 3) {
            constraint_str += "collate " + random_pick(scope->schema->available_collation);
        }

        // if (d6() == 1) 
        //     constraint_str += "not null ";
        // if (d6() == 1)
        //     constraint_str += "unique ";
        constraints.push_back(constraint_str);
    }

    // assign a primary key (so cannot use insert select, as it will automatically assign primary key value)
    if (table_engine.find("Log") == string::npos) {            // CLICKHOUSE: log engine does not support primary key
        if (table_option.find("WITHOUT ROWID") != string::npos // SQLITE: WITHOUT ROWID must has primary key
            || table_engine.find("MergeTree") != string::npos  // CLICKHOUSE: MergeTree engine must use either primary key or order by
            || d6() <= 5) { 
            
            has_primary_key = true;
            primary_col_id = dx(column_num) - 1;
            // primary_key_col = created_table->columns()[primary_col];
            // constraints[primary_col] += " PRIMARY KEY";
        }
    }

    // add table property
    if (d6() <= 5) {
        table_option = "";
        if (!scope->schema->available_table_options.empty()) {
            has_option = true;
            table_option += " " + random_pick(scope->schema->available_table_options);
        } 
        if (scope->schema->target_dbms == "yugabyte") {
            has_option = true;
            table_option += " " + yugabyte_table_option(this, created_table);
        }
        else if (scope->schema->target_dbms == "cockroach" && has_primary_key) {
            has_option = true;
            table_option += " " + cockroach_table_option(this, created_table, primary_col_id);
        }
    }

    if (!scope->schema->supported_table_engine.empty()) {
        has_engine = true;
        table_engine = random_pick(scope->schema->supported_table_engine);
    }
    
    

    // // check clause
    // if (d9() == 1) {
    //     has_check = true;
    //     scope->refs.push_back(&(*created_table));
    //     check_expr = bool_expr::factory(this);
    // }
}

void create_table_stmt::out(std::ostream &out)
{
    out << "create table ";
    out << created_table->name << " ( ";
    indent(out);

    auto columns_in_table = created_table->columns();
    int column_num = columns_in_table.size();
    for (int i = 0; i < column_num; i++) {
        out << columns_in_table[i].name << " ";
        out << columns_in_table[i].type->name << " ";
        out << constraints[i];
        if (i != column_num - 1)
            out << ",";
        indent(out);
    }

    if (has_primary_key) {
        out << ",";
        indent(out);
        out << "primary key(" << created_table->columns()[primary_col_id].name << ")";
    }

    if (has_check) {
        out << ",";
        indent(out);
        out << "check(" << *check_expr << ")";
    }
    indent(out);
    out << ")";

    if (has_option) {
        out << " " << table_option;
    }

    if (has_engine) {
        out << "engine = " << table_engine << endl;
    }
}

create_view_stmt::create_view_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    subquery = make_shared<struct query_spec>(this, scope);
    tatble_name = unique_table_name(scope);
}

void create_view_stmt::out(std::ostream &out)
{
    out << "create view " << tatble_name << " as ";
    indent(out);

    out << *subquery;
}

group_clause::group_clause(prod *p, struct scope *s, 
            shared_ptr<struct select_list> select_list,
            std::vector<shared_ptr<named_relation> > *from_refs)
: prod(p), myscope(s), modified_select_list(select_list)
{
    scope = &myscope;
    scope->tables = s->tables;

    auto& select_exprs = modified_select_list->value_exprs;
    auto& select_columns = modified_select_list->derived_table.columns();

    auto size = select_columns.size();

    target_ref = make_shared<column_reference>(this, (sqltype *)0, from_refs);

    // cannot use the ref of parent select
    auto tmp = scope->refs;
    scope->refs.clear();
    for (auto r : *from_refs)
        scope->refs.push_back(&*r);
    
    int tmp_group = use_group;
    use_group = 0; // cannot use aggregate function in aggregate function
    for (size_t i = 0; i < size; i++) {
        shared_ptr<value_expr> new_expr;
        if (i != 0)
            new_expr = make_shared<funcall>(this, (sqltype *)0, true);
        else {
            auto dot_index = target_ref->reference.find(".");
            assert(dot_index != string::npos && dot_index + 1 < target_ref->reference.length());
            string col_name = target_ref->reference.substr(dot_index + 1);
            new_expr = make_shared<column_reference>(this, target_ref->type, col_name, target_ref->table_ref);
        }
        select_exprs.erase(select_exprs.begin() + i);
        select_exprs.insert(select_exprs.begin() + i, new_expr);
        select_columns[i].type = new_expr->type;
    }
    use_group = tmp_group;
    scope->refs = tmp;
    
    // generating having search condition
    // erase the refs in from clause
    for (int i = 0; i < scope->refs.size(); i++) {
        bool in_from_clause = false;
        for (int j = 0; j < from_refs->size(); j++) {
            if (scope->refs[i] == (*from_refs)[j].get()) {
                in_from_clause = true;
                break;
            }
        }
        if (in_from_clause) {
            scope->refs.erase(scope->refs.begin() + i);
            i--;
        }
    }
    
    // create a new relation to contain target_ref col and other col with aggregate function
    for (int i = 0; i < from_refs->size(); i++) {
        auto& ref = (*from_refs)[i];
        auto new_relation = make_shared<named_relation>(ref->ident());
        for (auto& col : ref->columns()) {
            auto reference = ref->ident() + "." + col.name;
            if (reference == target_ref->reference) { // the columns used in target_def
                new_relation->columns().push_back(col);
                continue;
            }

            // not target_def, so need to wrapped by aggregate function
            for (auto& agg : scope->schema->aggregates) { // only invole the agg using one argument
                if (agg.argtypes.size() != 1)
                    continue;
                if (agg.argtypes.front() != col.type)
                    continue;
                new_relation->columns().push_back(column(col.name, col.type, &agg));
            }
        }
        tmp_store.push_back(new_relation);
        scope->refs.push_back(new_relation.get());
    }
    
    // build having clause
    having_cond_search = bool_expr::factory(this);
    
    // it use seperated my scope, do not need to restore the refs
    // scope->refs = tmp;
}

void group_clause::out(std::ostream &out)
{
    out << "group by " << *target_ref << " having " << *having_cond_search;
}

alter_table_stmt::alter_table_stmt(prod *parent, struct scope *s):
prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    int type_chosen = d9();
    if (type_chosen <= 3)
        stmt_type = 0;
    else if (type_chosen <= 6)
        stmt_type = 1;
    else 
        stmt_type = 2;

    // choose the base table (not view)
    int size = scope->tables.size();
    int chosen_table_idx = dx(size) - 1;
    named_relation *table_ref = NULL;
    table * real_table = NULL;
    while (1) {
        table_ref = scope->tables[chosen_table_idx];
        real_table = dynamic_cast<table *>(table_ref);
        if (real_table && real_table->is_base_table) {
            break;
        }
        chosen_table_idx = (chosen_table_idx + 1) % size;
    };
    
    set<string> exist_column_name;
    for (auto &c : table_ref->columns()) {
        exist_column_name.insert(upper_translate(c.name));
    }

    if (stmt_type == 0) { // rename table
        auto new_table_name = unique_table_name(scope);
        stmt_string = "alter table " + table_ref->ident() + " rename to " + new_table_name;
    }
    else if (stmt_type == 1) { // rename column
        column *column_ref;
        while (1) {
            column_ref = &random_pick(table_ref->columns());
            if (column_ref->name != PKEY_IDENT && column_ref->name != VKEY_IDENT)
                break;
        }

        auto new_column_name = unique_column_name();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        stmt_string = "alter table " + table_ref->ident() + " rename column " + column_ref->name
                        + " to " + new_column_name;
    }
    else if (stmt_type == 2) { // add column
        auto new_column_name = unique_column_name();
        while (exist_column_name.count(upper_translate(new_column_name))) {
            new_column_name = new_column_name + "_2";
        }

        vector<sqltype *> enable_type;
        enable_type.push_back(scope->schema->inttype);
        enable_type.push_back(scope->schema->realtype);
        enable_type.push_back(scope->schema->texttype);
        enable_type.push_back(scope->schema->datetype);
        auto type = random_pick<>(enable_type);

        stmt_string = "alter table " + table_ref->ident() + " add column " + new_column_name 
                        + " " + type->name;
    }
}

void alter_table_stmt::out(std::ostream &out)
{
    out << stmt_string;
}

drop_table_stmt::drop_table_stmt(prod *parent, struct scope *s):
prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    // choose the base table (not view)
    int size = scope->tables.size();
    int chosen_table_idx = dx(size) - 1;
    named_relation *table_ref = NULL;
    table * real_table = NULL;
    while (1) {
        table_ref = scope->tables[chosen_table_idx];
        real_table = dynamic_cast<table *>(table_ref);
        if (real_table && real_table->is_base_table) {
            break;
        }
        chosen_table_idx = (chosen_table_idx + 1) % size;
    };

    stmt_string = "drop table if exists " + table_ref->ident();
}

void drop_table_stmt::out(std::ostream &out)
{
    out << stmt_string;
}

create_index_stmt::create_index_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->indexes = s->indexes;

    is_unique = (d6() == 1);

    index_name = unique_index_name(scope);

    // only choose base table
    vector<named_relation *> base_tables;
    for (auto table : scope->tables) {
        auto t = dynamic_cast<struct table*>(table);
        if (t && t->is_base_table)
            base_tables.push_back(t);
    }
    auto indexed_table = random_pick(base_tables);
    indexed_table_name = indexed_table->name;

    // choosing indexed column
    // cannot use reference (i.e. &), because target_columns will erase
    auto target_columns = indexed_table->columns();
    
    auto indexed_num = dx(target_columns.size());
    while (indexed_num > 0) {
        auto choice = dx(target_columns.size()) - 1;
        auto& indexed_column = target_columns[choice];
        
        if (indexed_column.name == "rowid" && schema::target_dbms == "sqlite") {
            indexed_num--;
            target_columns.erase(target_columns.begin() + choice);
            continue;
        }

        indexed_column_names.push_back(indexed_column.name);

        // adding order
        auto asc_choice = d6();
        if (asc_choice <= 2)
            asc_desc_empty.push_back("asc");
        else if (asc_choice <= 4)
            asc_desc_empty.push_back("desc");
        else
            asc_desc_empty.push_back(" ");
        
        // adding collation
        if (!scope->schema->available_collation.empty() &&
                target_columns[choice].type == scope->schema->texttype &&
                d6() <= 3) {

            has_collation.push_back(true);
            collation.push_back(random_pick(scope->schema->available_collation));
        } else {
            string empty = " ";
            has_collation.push_back(false);
            collation.push_back(empty);
        }

        indexed_num--;
        target_columns.erase(target_columns.begin() + choice);
    }

    // adding where clause (partial index), cannot use subquery, so just use simple one (comparison)
    if (scope->schema->enable_partial_index && d6() <= 3) {
        scope->refs.push_back(indexed_table);

        auto &idx = scope->schema->operators_returning_type;
        auto iters = idx.equal_range(scope->schema->booltype);
        auto oper = random_pick<>(iters)->second;
        auto lhs = make_shared<column_reference>(this, oper->left);
        auto rhs = make_shared<const_expr>(this, oper->right); 
        where_expr = make_shared<comparison_op>(this, oper, lhs, rhs);
    }
}

void create_index_stmt::out(ostream &out)
{
    out << "create" << (is_unique ? " unique " : " ")
        << "index " << index_name << " on " << indexed_table_name << " (";
    
    int size = indexed_column_names.size();
    for (int i = 0; i < size; i++) {
        out << indexed_column_names[i] << " ";

        if (has_collation[i])  // has collation
            out << "collate " << collation[i] << " ";

        out << asc_desc_empty[i];
        if (i + 1 < size)
            out << ", ";
    }
    out << ")";
    if (where_expr) // check whether has where_expr
        out << " where " << *where_expr;
}

create_trigger_stmt::create_trigger_stmt(prod *parent, struct scope *s)
: prod(parent), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    trigger_name = unique_table_name(scope);
    trigger_time = d6() <= 4 ? "after" : "before";
    
    auto choose = d9();
    if (choose <= 3)
        trigger_event = "insert";
    else if (choose <= 6)
        trigger_event = "update";
    else
        trigger_event = "delete";
    
    // only choose base table
    auto tables_size = s->tables.size();
    size_t chosen_one = dx(tables_size) - 1;
    while (1) {
        auto chosen_table = dynamic_cast<struct table*>(s->tables[chosen_one]);
        if (chosen_table && chosen_table->is_base_table)
            break;
        chosen_one++;
        if (chosen_one == tables_size)
            chosen_one = 0;
    }
    table_name = s->tables[chosen_one]->ident();

    int stmts_num = (d6() + 1) / 2; // 1 - 3
    for (int i = 0; i < stmts_num; i++) {
        shared_ptr<struct modifying_stmt> doing_stmt;
        choose = d9();
        if (choose <= 3)
            doing_stmt = make_shared<insert_stmt>(this, scope);
        else if (choose <= 6)
            doing_stmt = make_shared<update_stmt>(this, scope);
        else
            doing_stmt = make_shared<delete_stmt>(this, scope);
        
        doing_stmts.push_back(doing_stmt);
    }
    
}

void create_trigger_stmt::out(std::ostream &out)
{
    out << "create trigger " << trigger_name << " ";
    out << trigger_time << " ";
    out << trigger_event << " on " << table_name;
    indent(out);
    out << "for each row";
    indent(out);
    out << "begin ";
    indent(out);
    for (auto &stmt : doing_stmts) {
        out << *stmt << "; ";
        indent(out);
    }
    out << "end";
}

named_window::named_window(prod *p, struct scope *s):
 prod(p), myscope(s)
{
    scope = &myscope;
    scope->tables = s->tables;

    window_name = unique_window_name();

    int partition_num = d6() > 4 ? 2 : 1;
    while (partition_num > 0) {
        shared_ptr<value_expr> partition_expr;
        if (schema::target_dbms == "clickhouse")
            partition_expr = make_shared<column_reference>(this);
        else
            partition_expr = value_expr::factory(this);
        if (!dynamic_pointer_cast<const_expr>(partition_expr)) {
            partition_by.push_back(partition_expr);
            partition_num--;
        }
    }

    // order by all possible col ref, make the result determined
    for (auto r : scope->refs) {
        for (auto& c : (*r).columns()) {
            // if (schema::target_dbms == "sqlite" && c.name == "rowid")
            //     continue;
            auto col = make_shared<column_reference>(this, c.type, c.name, r->name);
            auto is_asc = d6() <= 3 ? true : false;
            order_by.push_back(make_pair<>(col, is_asc));
        }
    }
}

void named_window::out(std::ostream &out)
{
    out << "window " << window_name << " as (partition by ";
    for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++) {
        out << **ref;
        if (ref + 1 != partition_by.end())
            out << ",";
    }
    if (!order_by.empty()) {
        out << " order by ";
        for (auto ref = order_by.begin(); ref != order_by.end(); ref++) {
            auto& order_pair = *ref;
            out << *(order_pair.first) << " ";
            out << (order_pair.second ? "asc" : "desc");
            if (ref + 1 != order_by.end())
                out << ", ";
        }
    }
    out << ")";
}

unioned_query::unioned_query(prod *p, struct scope *s, bool lateral, vector<sqltype *> *pointed_type):
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    if (lateral)
        scope->refs = s->refs;
    
    lhs = make_shared<query_spec>(this, this->scope, lateral, pointed_type);
    vector<sqltype *> tmp_type_vec;
    auto &lhs_exprs = lhs->select_list->value_exprs;
    for (auto iter = lhs_exprs.begin(); iter != lhs_exprs.end(); iter++) {
        tmp_type_vec.push_back((*iter)->type);
    }
    rhs = make_shared<query_spec>(this, this->scope, lateral, &tmp_type_vec);

    lhs->has_order = false;
    rhs->has_order = false;
    lhs->has_limit = false;
    rhs->has_limit = false;

    assert(!scope->schema->compound_operators.empty());
    type = random_pick(scope->schema->compound_operators);
}

unioned_query::unioned_query(prod *p, struct scope *s, shared_ptr<query_spec> q_lhs, shared_ptr<query_spec> q_rhs, string u_type):
  prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones
    scope->tables = s->tables;
    lhs = q_lhs;
    rhs = q_rhs;
    type = u_type;

    lhs->has_order = false;
    rhs->has_order = false;
    lhs->has_limit = false;
    rhs->has_limit = false;
}

void unioned_query::equivalent_transform() {
    assert(!is_transformed);
    is_transformed = true;
    
    if (lhs->has_group || lhs->has_window || rhs->has_group || rhs->has_window) { // keep as the same
        eq_query = make_shared<unioned_query>(this, scope, lhs, rhs, type);
        return;
    }

    if (type == "intersect" || type == "except") {
        op* eq = NULL;
        for (auto& op : scope->schema->operators) {
            if (op.name == "=")
                eq = &op;
        }
        assert(eq);
        
        shared_ptr<bool_term> intersect_predicate;
        auto list_size = lhs->select_list->value_exprs.size();
        for (int i = 0; i < list_size; i++) {
            auto q1_expr = lhs->select_list->value_exprs[i];
            auto q2_expr = rhs->select_list->value_exprs[i];

            auto q1_eq_q2 = make_shared<comparison_op>(this, eq, q1_expr, q2_expr);

            auto q1_expr_is_null = make_shared<null_predicate>(this, q1_expr, true);
            auto q2_expr_is_null = make_shared<null_predicate>(this, q2_expr, true);
            auto null_and = make_shared<bool_term>(this, false, q1_expr_is_null, q2_expr_is_null);

            auto eq_or_null = make_shared<bool_term>(this, true, q1_eq_q2, null_and);
            if (intersect_predicate) {
                intersect_predicate = make_shared<bool_term>(this, false, intersect_predicate, eq_or_null);
            } else {
                intersect_predicate = eq_or_null;
            }
        }

        rhs->search = make_shared<bool_term>(this, false, intersect_predicate, rhs->search);
        auto exists_rhs = make_shared<exists_predicate>(this, rhs);
        shared_ptr<bool_expr> updated_q1_predicate;
        if (type == "intersect")
            updated_q1_predicate = make_shared<bool_term>(this, false, exists_rhs, lhs->search);
        else { // except
            auto not_exists = make_shared<not_expr>(this, exists_rhs);
            updated_q1_predicate = make_shared<bool_term>(this, false, not_exists, lhs->search);
        }
        lhs->search = updated_q1_predicate;
        eq_query = lhs;
        return;
    }

    // keep as the same
    eq_query = make_shared<unioned_query>(this, scope, lhs, rhs, type);
    return;
}

void unioned_query::back_transform() {
    assert(is_transformed);
    is_transformed = false;

    if (lhs->has_group || lhs->has_window || rhs->has_group || rhs->has_window)
        return;
    if (type == "union" || type == "union all")
        return;
    if (type == "intersect" || type == "except") {
        auto rhs_and_op = dynamic_pointer_cast<bool_term>(rhs->search);
        rhs->search = dynamic_pointer_cast<bool_expr>(rhs_and_op->rhs);
        assert(rhs->search);

        auto updated_q1_predicate = dynamic_pointer_cast<bool_term>(lhs->search);
        lhs->search = dynamic_pointer_cast<bool_expr>(updated_q1_predicate->rhs);
        assert(lhs->search);

        return;
    }
}

void unioned_query::out(std::ostream &out) {
    if (!is_transformed) {
        out << *lhs;
        indent(out);
        out << type;
        indent(out);
        out << *rhs;
    } else {
        out << *eq_query;
    }
}

insert_select_stmt::insert_select_stmt(prod *p, struct scope *s, table *v)
  : modifying_stmt(p, s, v)
{
    match();

    // dont select the target table
    vector<named_relation *> excluded_tables;
    vector<pair<sqltype*, table*>> excluded_t_with_c_of_type;
    exclude_tables(victim, scope->tables, 
                    scope->schema->tables_with_columns_of_type, 
                    excluded_tables, 
                    excluded_t_with_c_of_type);
    
    // can only select base table, because views may include the data of target table
    vector<table*> views;
    for (auto t : scope->tables) {
        auto real_t = dynamic_cast<table*>(t);
        if (real_t && real_t->is_base_table == false)
            views.push_back(real_t);
    }
    for (auto v : views) {
        exclude_tables(v, scope->tables, scope->schema->tables_with_columns_of_type, 
                        excluded_tables, excluded_t_with_c_of_type);
    }

    vector<sqltype *> pointed_type;
    // select all columns to eliminate all non-determinism
    for (auto& col : victim->columns()) {
        valued_column_name.push_back(col.name);
        pointed_type.push_back(col.type);
    }

    target_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);

    // Do not recover, because equivalent transform may also use the scope 
    // And recovering is unnecessary.
    // recover_tables(scope->tables, 
    //                 scope->schema->tables_with_columns_of_type, 
    //                 excluded_tables, 
    //                 excluded_t_with_c_of_type);
}

void insert_select_stmt::out(std::ostream &out)
{
    out << "insert into " << victim->ident()<< " (";
    auto col_num = valued_column_name.size(); 
    for (int i = 0; i < col_num; i++) {
        out << valued_column_name[i];
        if (i + 1 < col_num)
            out << ", ";
    }
    out << ") ";
    indent(out);
    out << *target_subquery;
}

analyze_stmt::analyze_stmt(prod* p, struct scope *s) : prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones

    vector<struct table *> available_t;
    for (auto t : scope->tables) {
        auto table = dynamic_cast<struct table *>(t);
        if (table->is_base_table)
            available_t.push_back(table);

    }
    target = random_pick(available_t)->name;
}

void analyze_stmt::out(std::ostream &out) 
{
    out << "ANALYZE ";
    if (scope->schema->target_dbms == "tidb")
        out << "TABLE ";
    out << target;
}

set_stmt::set_stmt(prod* p, struct scope *s) : prod(p), myscope(s)
{
    scope = &myscope; // isolate the scope, dont effect the upper ones

    auto rand = dx(scope->schema->supported_setting.size()) - 1;
    auto it = scope->schema->supported_setting.begin();
    advance(it, rand);
    parm = it->first;
    value = random_pick<>(it->second);
}

void set_stmt::out(std::ostream &out) 
{
    out << "SET " << parm << " = " << value;
}

shared_ptr<prod> statement_factory(struct scope *s)
{
    try {
        s->new_stmt();
        // if less than 2 tables, update_stmt will easily enter a dead loop.
        if (s->tables.size() < 2) { 
            if (s->tables.empty() || d6() > 3)
                return make_shared<create_table_stmt>((struct prod *)0, s);
            else
                return make_shared<create_view_stmt>((struct prod *)0, s);
        }

        auto choice = d20();
        if (s->tables.empty() || choice == 1)
            return make_shared<create_table_stmt>((struct prod *)0, s);
        #ifndef TEST_CLICKHOUSE
        #ifndef TEST_TIDB
        if (choice == 2)
            return make_shared<create_table_select_stmt>((struct prod *)0, s);
        #endif
        if (choice == 3)
            return make_shared<alter_table_stmt>((struct prod *)0, s);
        if (choice == 18)
            return make_shared<drop_table_stmt>((struct prod *)0, s);
        if (choice == 4)
            return make_shared<delete_stmt>((struct prod *)0, s);
        if (choice == 5) 
            return make_shared<update_stmt>((struct prod *)0, s);
        if (choice == 6)
            return make_shared<create_index_stmt>((struct prod *)0, s);
        #else
        if (choice >= 2 && choice <= 5)
            return make_shared<drop_table_stmt>((struct prod *)0, s);
        #endif
        
        #if (!defined TEST_MONETDB) && (!defined TEST_PGSQL) && (!defined TEST_CLICKHOUSE)
        if (choice == 7)
            return make_shared<create_trigger_stmt>((struct prod *)0, s);
        #endif
        if (choice == 8)
            return make_shared<insert_stmt>((struct prod *)0, s);
        if (choice == 9)
            return make_shared<insert_select_stmt>((struct prod *)0, s);
        if (choice >= 10 && choice <= 12)
            return make_shared<common_table_expression>((struct prod *)0, s);
        if (choice >= 13 && choice <= 15)
            return make_shared<unioned_query>((struct prod *)0, s);
        return make_shared<query_spec>((struct prod *)0, s);
    } catch (runtime_error &e) {
        cerr << "catch a runtime error" << endl;
        return statement_factory(s);
    }
}

shared_ptr<prod> ddl_statement_factory(struct scope *s)
{
#define LEAST_TABLE_NUM 3
    try {
        s->new_stmt();
        // if less than 3 tables, update_stmt will easily enter a dead loop.
        if (s->tables.size() < LEAST_TABLE_NUM)
            return make_shared<create_table_stmt>((struct prod *)0, s);

        auto choice = d6();
        if (choice == 1)
            return make_shared<create_table_stmt>((struct prod *)0, s);
        if (choice == 2)
            return make_shared<create_view_stmt>((struct prod *)0, s);

        // create_index is not in here, because index is activated only after some data has been inserted
        // so create_index is in basic_dml_statement_factory
        
        // do not alter table, because dropping table will make existing view invalid
        // if (choice == 4)
        //     return make_shared<alter_table_stmt>((struct prod *)0, s);

        // // database has at least LEAST_TABLE_NUM tables in case dml statements are used
        // if (choice == 5 && s->tables.size() > LEAST_TABLE_NUM) 
        //     return make_shared<drop_table_stmt>((struct prod *)0, s);

        #if (!defined TEST_MONETDB) && (!defined TEST_PGSQL) && (!defined TEST_CLICKHOUSE) && (!defined TEST_TIDB)
        if (choice == 6)
            return make_shared<create_trigger_stmt>((struct prod *)0, s);
        #endif
        
        return ddl_statement_factory(s);

    } catch (runtime_error &e) {
        cerr << "catch a runtime error in " << __FUNCTION__  << endl;
        cerr << e.what() << endl;
        return ddl_statement_factory(s);
    }
}

shared_ptr<prod> basic_dml_statement_factory(struct scope *s)
{
    try { // only use insert_stmt to add data to target table
        s->new_stmt();
        auto choice = d42();
        if (s->schema->enable_analyze_stmt && choice <= 1)
            return make_shared<analyze_stmt>((struct prod *)0, s);
        else if (s->schema->target_dbms != "clickhouse" && choice <= 2) // CLICKHOUSE does not support CREATE INDEX
            return make_shared<create_index_stmt>((struct prod *)0, s);
        else 
            return make_shared<insert_stmt>((struct prod *)0, s, (table *)NULL, true);

    } catch (runtime_error &e) {
        cerr << "err: " << e.what() << endl;
        cerr << "catch a runtime error in " << __FUNCTION__  << endl;
        return basic_dml_statement_factory(s);
    }
}

shared_ptr<prod> txn_statement_factory(struct scope *s, int choice)
{
    static int recur_time = 0;
    try {
        s->new_stmt();
        if (choice == -1)
            choice = d12();
        // should not have ddl statement, which will auto commit in tidb;
        if (choice <= 2)
            return make_shared<delete_stmt>((struct prod *)0, s);
        else if (choice == 3)
            return make_shared<common_table_expression>((struct prod *)0, s, true);
        else if (choice == 4)
            return make_shared<query_spec>((struct prod *)0, s, false, (vector<sqltype *> *)NULL, true);
        else if (choice <= 7)
            return make_shared<insert_stmt>((struct prod *)0, s);
        else
            return make_shared<update_stmt>((struct prod *)0, s);
        // return txn_statement_factory(s);
    } catch (runtime_error &e) {
        string err = e.what();
        cerr << "catch a runtime error: " << err << endl;
        recur_time++;
        if (recur_time > 10)
            exit(-1);
        auto ret = txn_statement_factory(s, choice);
        recur_time--;
        return ret;
    }
}