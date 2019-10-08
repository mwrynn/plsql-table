CREATE OR REPLACE PACKAGE BODY pkg_table_obj_test AS
  test_table_obj table_obj;
  
  PROCEDURE setup(schema IN VARCHAR2 DEFAULT 'MWRYNN') AS
    test_schema VARCHAR2(32767);
  BEGIN
    test_schema := UPPER(schema);
    
    --tried defaulting to sys_context( 'userenv', 'current_schema' ), but that returns null in this context

    test_table_obj := table_obj('MW_TEST', UPPER(test_schema), NULL);
    
    --I can't believe we still don't have DROP TABLE IF EXISTS!
    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || test_schema || '.mw_test';
    EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
    END;

    --if I use test_table_obj.schema_name below instead of test_schema, internal error. ¯\_(ツ)_/¯
    EXECUTE IMMEDIATE 'CREATE TABLE ' || test_schema || '.mw_test(a INT, b VARCHAR2(10), c DATE, d NUMBER(15,2), PRIMARY KEY(a,d))';
    
  END;
  
  PROCEDURE test_qual_table_name_nodblink IS
    qual_table_name VARCHAR(32767);
  BEGIN
    ut3.ut.expect(test_table_obj.qual_table_name).to_equal(test_table_obj.schema_name || '.MW_TEST');
  END;
  
  PROCEDURE test_all_cols IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.all_cols)).to_equal('A,B,C,D');
  END;
  
  PROCEDURE test_all_cols_exclude_list IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.all_cols(exclude_list => COLS_ARR('B','D')))).to_equal('A,C');
  END;
  
  PROCEDURE test_all_cols_alias IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.all_cols(alias => 'alias'))).to_equal('ALIAS.A,ALIAS.B,ALIAS.C,ALIAS.D');
  END;
  
  PROCEDURE test_pk_cols IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.pk_cols)).to_equal('A,D');
  END;
END;

