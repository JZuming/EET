## Reported Bugs in TiDB
1. Panic triggered at expression.ExplainExpressionList (planner/core/explain.go:335)
    - Link: https://github.com/pingcap/tidb/issues/42587
    - Status: Confirmed
2. Panic triggered at chunk.appendCellByCell (util/chunk/chunk.go:421)
    - Link: https://github.com/pingcap/tidb/issues/42588
    - Status: Fixed
3. Incorrect results of SELECT caused by subquery and logical operations
    - Link: https://github.com/pingcap/tidb/issues/42617
    - Status: Confirmed
4. Internal error triggered at expression.ExpressionsToPBList (expression/expr_to_pb.go:42)
    - Link: https://github.com/pingcap/tidb/issues/42622
    - Status: Fixed
5. Panic triggered at core.(*LogicalProjection).TryToGetChildProp (planner/core/exhaust_physical_plans.go:2500)
    - Link: https://github.com/pingcap/tidb/issues/42734
    - Status: Fixed
6. Panic triggered at core.(*LogicalJoin).BuildKeyInfo (planner/core/rule_build_key_info.go:201)
    - Link: https://github.com/pingcap/tidb/issues/42736
    - Status: Confirmed
7. Panic triggered at chunk.(*Column).AppendFloat64 (util/chunk/column.go:265)
    - Link: https://github.com/pingcap/tidb/issues/42737
    - Status: Confirmed
8. Panic triggered at expression.(*CorrelatedColumn).Eval (expression/column.go:88)
    - Link: https://github.com/pingcap/tidb/issues/42739
    - Status: Fixed
9. Panic triggered at expression.(*builtinRpadUTF8Sig).evalString (expression/builtin_string.go:2278)
    - Link: https://github.com/pingcap/tidb/issues/42770
    - Status: Confirmed
10. Incorrect results of SELECT caused by JOIN table
    - Link: https://github.com/pingcap/tidb/issues/42821
    - Status: Confirmed