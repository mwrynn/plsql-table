CREATE OR REPLACE PACKAGE BODY pkg_table_obj_test AS
  test_table_obj table_obj;
  test_parent_table_obj table_obj;

  
  PROCEDURE setup(schema IN VARCHAR2 DEFAULT 'MWRYNN') AS
    test_schema VARCHAR2(32767);
  BEGIN
    test_schema := UPPER(schema);
    
    --tried defaulting to sys_context( 'userenv', 'current_schema' ), but that returns null in this context

    test_table_obj := table_obj('MW_TEST', UPPER(test_schema), NULL);
    test_parent_table_obj := table_obj('MW_TEST_PARENT', UPPER(test_schema), NULL);
        
    test_table_obj.drop_table;
    test_parent_table_obj.drop_table;

    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || test_schema || '.mw_test_parent';
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

    EXECUTE IMMEDIATE 'CREATE TABLE ' || test_schema || '.mw_test_parent(b VARCHAR2(10), x TIMESTAMP, PRIMARY KEY(b))';

    --add fk reference of mw_test to mw_test_parent
    EXECUTE IMMEDIATE 'ALTER TABLE ' || test_schema || '.mw_test ADD CONSTRAINT fk_mw_test FOREIGN KEY (b) REFERENCES ' || test_schema || '.mw_test_parent(b)';

    --insert a little bit of dummy data into both tables
    EXECUTE IMMEDIATE 'INSERT INTO ' || test_schema || '.mw_test_parent(b,x) VALUES (''abc'', to_timestamp(''20190314'',''YYYYMMDD''))';
    EXECUTE IMMEDIATE 'INSERT INTO ' || test_schema || '.mw_test_parent(b,x) VALUES (''def'', to_timestamp(''20190212'',''YYYYMMDD''))';
    EXECUTE IMMEDIATE 'INSERT INTO ' || test_schema || '.mw_test(a,b,c,d) VALUES (1, ''abc'', to_date(''20190101'',''YYYYMMDD''), 543.21)';
    EXECUTE IMMEDIATE 'INSERT INTO ' || test_schema || '.mw_test(a,b,c,d) VALUES (2, ''abc'', to_date(''20190401'',''YYYYMMDD''), 123456.7)';
    EXECUTE IMMEDIATE 'INSERT INTO ' || test_schema || '.mw_test(a,b,c,d) VALUES (3, ''def'', to_date(''20190325'',''YYYYMMDD''), 0)';

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

  PROCEDURE test_gen_insert_random_rows_stmt IS
    random_rows_stmt LONG;
    insert_regex LONG;
    values_regex LONG;
  BEGIN
    random_rows_stmt := test_table_obj.gen_insert_random_rows_stmt(3, to_date('20190101', 'YYYYMMDD'), to_date('20190131', 'YYYYMMDD'));

    --random data should be as follows (a sample):

    --INSERT INTO MWRYNN.MW_TEST(A,B,C,D)
    --SELECT 4277993685,'abc',to_date('20190207 00:00:00', 'yyyymmdd hh24:mi:ss'),5520739623042.72 FROM dual
    -- UNION ALL
    --SELECT 6148461452,'abc',to_date('20190222 00:00:00', 'yyyymmdd hh24:mi:ss'),3996397054533.44 FROM dual
    -- UNION ALL
    --SELECT 5347208714,'def',to_date('20190204 00:00:00', 'yyyymmdd hh24:mi:ss'),8607663790748.64 FROM dual

    values_regex := 'SELECT [0-9]+,''(abc|def)'',to_date\(''[0-9]{8} [0-9]{2}:[0-9]{2}:[0-9]{2}'', ''yyyymmdd hh24:mi:ss''\),[0-9]+[\.]{0,1}[0-9]{0,2} FROM dual';

    insert_regex := 'INSERT INTO ' || test_table_obj.qual_table_name || '\(A,B,C,D\)' || chr(10) ||
      values_regex || chr(10) ||
      ' UNION ALL' || chr(10) ||
      values_regex || chr(10) ||
      ' UNION ALL' || chr(10) ||
      values_regex;

    ut3.ut.expect(regexp_like(random_rows_stmt, insert_regex)).to_equal(true);
  END;

  PROCEDURE test_gen_insert_random_rows_stmt_date_fail IS
    dummy LONG;
  BEGIN
    dummy := test_table_obj.gen_insert_random_rows_stmt(3);
  END;

  PROCEDURE test_drop_table IS
  BEGIN
    test_table_obj.drop_table;

    ut3.ut.expect(test_table_obj.table_exists).to_equal(false);
  END;
END;

