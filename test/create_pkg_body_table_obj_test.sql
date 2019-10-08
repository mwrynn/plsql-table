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
    
    EXECUTE IMMEDIATE 'CREATE INDEX ' || test_schema || '.idx_mw_test_1 ON ' || test_schema || '.mw_test(b)';
    EXECUTE IMMEDIATE 'CREATE INDEX ' || test_schema || '.idx_mw_test_2 ON ' || test_schema || '.mw_test(c)';

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

  PROCEDURE test_all_cols_exclude_list_and_alias IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.all_cols(alias => 'alias', exclude_list => COLS_ARR('A')))).to_equal('ALIAS.B,ALIAS.C,ALIAS.D');
  END;

  PROCEDURE test_pk_cols IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.pk_cols)).to_equal('A,D');
  END;

  PROCEDURE test_non_pk_cols IS
  BEGIN
    ut3.ut.expect(UPPER(test_table_obj.non_pk_cols)).to_equal('B,C');
  END;

  PROCEDURE test_disable_indexes IS
    all_are_disabled BOOLEAN := true;
    disabled_count_from_func INT;
    disabled_count_for_test INT := 0;
  BEGIN
    --check that all non-unique indexes are marked as unusable per all_indexes
    disabled_count_from_func := test_table_obj.disable_indexes;
    FOR rec IN (SELECT status FROM all_indexes WHERE uniqueness='NONUNIQUE' AND owner=test_table_obj.schema_name AND table_name=test_table_obj.table_name) LOOP
      IF rec.status != 'UNUSABLE' THEN
        all_are_disabled := false;
      ELSE
        disabled_count_for_test := disabled_count_for_test + 1;
      END IF;
    END LOOP;

    ut3.ut.expect(all_are_disabled).to_equal(true);
    ut3.ut.expect(disabled_count_from_func).to_equal(disabled_count_for_test);
  END;

  PROCEDURE test_enable_indexes IS
    all_are_enabled BOOLEAN := true;
    disabled_count_from_func INT;
    enabled_count_from_func INT;
    enabled_count_for_test INT := 0;
  BEGIN
    --first disable all indexes
    disabled_count_from_func := test_table_obj.disable_indexes;

    --check that all non-unique indexes are marked as valid per all_indexes
    enabled_count_from_func := test_table_obj.enable_indexes;
    FOR rec IN (SELECT status FROM all_indexes WHERE uniqueness='NONUNIQUE' AND owner=test_table_obj.schema_name AND table_name=test_table_obj.table_name) LOOP
      IF rec.status != 'VALID' THEN
        all_are_enabled := false;
      ELSE
        enabled_count_for_test := enabled_count_for_test + 1;
      END IF;
    END LOOP;

    ut3.ut.expect(all_are_enabled).to_equal(true);
    ut3.ut.expect(enabled_count_from_func).to_equal(enabled_count_for_test);
    ut3.ut.expect(disabled_count_from_func).to_equal(enabled_count_from_func);
  END;
END;

