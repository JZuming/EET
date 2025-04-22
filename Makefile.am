-include Makefile.local

bin_PROGRAMS = eet

AM_CPPFLAGS = -Dfuzz_test
DUT = postgres.cc yugabyte.cc

if DUT_SQLITE
DUT += sqlite.cc
AM_CPPFLAGS += -DHAVE_SQLITE
endif

if DUT_TIDB
DUT += tidb.cc
AM_CPPFLAGS += -DHAVE_TIDB
endif

if DUT_MYSQL
DUT += mysql.cc
AM_CPPFLAGS += -DHAVE_MYSQL
endif

DUT += clickhouse.cc
AM_CPPFLAGS += -DHAVE_CLICKHOUSE

QCN_TESTER_SRC = qcn_tester/qcn_tester.cc \
                qcn_tester/qcn_select_tester.cc \
                qcn_tester/qcn_update_tester.cc \
                qcn_tester/qcn_cte_tester.cc \
                qcn_tester/qcn_delete_tester.cc \
                qcn_tester/qcn_insert_select_tester.cc

VALUE_EXPR_SRC = \
value_expr/atomic_subselect.cc \
value_expr/window_function.cc \
value_expr/value_expr.cc\
value_expr/coalesce.cc \
value_expr/funcall.cc \
value_expr/win_func_using_exist_win.cc \
value_expr/binop_expr.cc \
value_expr/const_expr.cc \
value_expr/case_expr.cc \
value_expr/win_funcall.cc \
value_expr/printed_expr.cc \
value_expr/column_reference.cc 

BOOL_EXPR_SRC = \
value_expr/bool_expr/comp_subquery.cc \
value_expr/bool_expr/not_expr.cc \
value_expr/bool_expr/like_op.cc \
value_expr/bool_expr/between_op.cc \
value_expr/bool_expr/null_predicate.cc \
value_expr/bool_expr/bool_expr.cc \
value_expr/bool_expr/exists_predicate.cc \
value_expr/bool_expr/const_bool.cc \
value_expr/bool_expr/in_query.cc \
value_expr/bool_expr/bool_binop/distinct_pred.cc \
value_expr/bool_expr/bool_binop/bool_term.cc \
value_expr/bool_expr/bool_binop/bool_binop.cc \
value_expr/bool_expr/bool_binop/comparison_op.cc

AUTOMAKE_OPTIONS = subdir-objects

eet_SOURCES = relmodel.cc schema.cc $(DUT)	 			\
    random.cc prod.cc $(VALUE_EXPR_SRC) $(BOOL_EXPR_SRC) grammar.cc log.cc dump.cc impedance.cc	\
    dbms_info.cc general_process.cc $(QCN_TESTER_SRC) qcn.cc

eet_LDADD = $(LIBPQXX_LIBS) $(MONETDB_MAPI_LIBS) $(BOOST_REGEX_LIB) $(POSTGRESQL_LIBS) $(BOOST_LDFLAGS) $(POSTGRESQL_LDFLAGS)

# AM_LDFLAGS = -fsanitize=address
AM_CPPFLAGS += $(BOOST_CPPFLAGS) $(LIBPQXX_CFLAGS) $(POSTGRESQL_CPPFLAGS) $(MONETDB_MAPI_CFLAGS) -Wall -Wno-sign-compare -Wextra -fPIC -g 
#-O0 -fsanitize=address -fno-omit-frame-pointer
