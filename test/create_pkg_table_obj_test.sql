CREATE OR REPLACE PACKAGE pkg_table_obj_test AS
  --%suite(tests for the table_obj functions)
  --%rollback(manual)
  
  --%beforeall
  PROCEDURE setup(schema IN VARCHAR2 DEFAULT 'MWRYNN');
  
  --%test(test that the function qual_table_name, without a dblink, returns the expected string)
  PROCEDURE test_qual_table_name_nodblink;
  
  --%test(test that the function all_cols, no alias, no exclude_list, returns the list of all table columns)
  PROCEDURE test_all_cols;
  
  --%test(test that the function all_cols, no alias, with an exclude_list, returns the list of all table columns except the excluded ones)
  PROCEDURE test_all_cols_exclude_list;
  
  --%test(test that the function all_cols, with an alias, no exclude_list, returns the list of all table columns with the alias)
  PROCEDURE test_all_cols_alias;
  
  --%test(test that the function all_cols, with an alias, and with exclude_list, returns the list of all table columns with the alias except for the excluded ones)
  PROCEDURE test_all_cols_exclude_list_and_alias;

  --%test(test that the function pk_cols, no an alias, no exclude_list, returns the list of all primary key columns)
  PROCEDURE test_pk_cols;

  --%test(test that the function non_pk_cols, no an alias, no exclude_list, returns the list of all non-primary key columns)
  PROCEDURE test_non_pk_cols;

  --%test(test that the function disable_indexes disables all non-unique indexes on the table)
  PROCEDURE test_disable_indexes;

  --%test(test that the function enable_indexes enables all non-unique indexes on the table)
  PROCEDURE test_enable_indexes;

  --%test(test that the function gen_insert_random_rows_stmt generates valid data)
  PROCEDURE test_gen_insert_random_rows_stmt;

  --%test(test that the function gen_insert_random_rows_stmt fails when date range not specified, when the table has a date column)
  --%throws(-20005)
  PROCEDURE test_gen_insert_random_rows_stmt_date_fail;

  --%test(test that diff gives valid diffs in two tables with a few column differences)
  PROCEDURE test_diff_cols;

  --%test(test that no column diffs found in two tables with identical columns)
  PROCEDURE test_diff_cols_same;

  --%test(test that existing table is successfully dropped)
  PROCEDURE test_drop_table;
END;

