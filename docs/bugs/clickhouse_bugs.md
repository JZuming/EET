## Reported Bugs in ClickHouse
1. Wrong result of the boolean expression with JIT (compile_expressions) while comparing NaNs
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50039
    - Status: Fixed
2. Wrong results of SELECT statements caused by OUTER JOIN
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50048
    - Status: Confirmed
3. Wrong results of the comparison for NaN values
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50075
    - Status: Confirmed
4. Wrong results of SELECT DISTINCT statement when comparing 0 and -0
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50192
    - Status: Reported
5. Wrong result of SELECT statement with compile_expressions
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50198
    - Status: Fixed
6. Unexpected error: Cannot find column ... in ActionsDAG result
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50227
    - Status: Confirmed
7. LOGICAL_ERROR: Argument column doesn't match result column
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50266
    - Status: Fixed
8. Segmentation fault at DB::FunctionComparison<DB::LessOp, DB::NameLess>::executeString
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50236
    - Status: Fixed
9. Wrong result of SELECT statement with compile_expressions in recent commits (head version)
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50269
    - Status: Fixed
10. Trigger INVALID_JOIN_ON_EXPRESSION while the SQL does not use ON clause
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50271
    - Status: Fixed
11. Undetermined results of SELECT statement using bitShiftRight
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50281
    - Status: Confirmed
12. Unexpected error: Amount of memory requested to allocate is more than allowed (CANNOT_ALLOCATE_MEMORY)
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50282
    - Status: Fixed
13. Wrong result of SELECT statement with compile_expressions in recent commits (affected version: 21-23.5.1)
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50283
    - Status: Fixed
14. LOGICAL_ERROR: Bad cast from type DB::ColumnVector<char8_t> to DB::ColumnVector<signed char>
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50284
    - Status: Fixed
15. Unexpected error (Unknown identifier: 0) when using bitOr and width_bucket
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50316
    - Status: Fixed
16. Crash at llvm::X86InstrInfo::copyPhysReg
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50323
    - Status: Fixed
17. Wrong results of SELECT statement comparing 1 and NaN
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50349
    - Status: Confirmed
18. Inconsistent results caused by bitShiftLeft after recent fix
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50488
    - Status: Fixed
19. Incorrect SELECT statement using toYear() in head version
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50520
    - Status: Fixed
20. Server crashes triggered at DB::FunctionMathUnary
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50530
    - Status: Fixed
21. Unexpected error: Not found column NULL in block
    - Link: https://github.com/ClickHouse/ClickHouse/issues/50582
    - Status: Fixed