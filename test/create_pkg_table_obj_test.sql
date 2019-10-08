CREATE OR REPLACE PACKAGE pkg_table_obj_test AS
  --%suite(tests for the table_obj functions)
  
  --%beforeall
  --%rollback(manual)
  PROCEDURE setup(schema IN VARCHAR2 DEFAULT 'MWRYNN');
  
  --%test(test that the function qual_table_name, without a dblink, returns the expected string)
  PROCEDURE test_qual_table_name_nodblink;
  
  --%test(test that the function all_cols, no alias, no exclude_list, returns the list of all table columns)
  PROCEDURE test_all_cols;
  
  --%test(test that the function all_cols, no alias, with an exclude_list, returns the list of all table columns except the excluded ones)
  PROCEDURE test_all_cols_exclude_list;
  
  --%test(test that the function all_cols, with an alias, no exclude_list, returns the list of all table columns with the alias)
  PROCEDURE test_all_cols_alias;
  
  --%test(test that the function pk_cols, no an alias, no exclude_list, returns the list of all primary key columns)
  PROCEDURE test_pk_cols;

END;

