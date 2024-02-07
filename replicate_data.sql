------------------------------------------------------------------------------
--
--  Replicate data between DB instances or schemas

--  Procedure p_replicate_tables() replicates data from all tables in the current Oracle schema
--  to another database or schema. Tables must exist and be in the same structure,
--  but partitions from source DB are created in target DB.
--  Purpose: to keep DB environments consistent e.g. to replicate PROD  to TEST or DEV)
--
--  Replication strategy is: catch exceptions on each tables, log errors but proceed,
--  to replicate as much as possible even if individual tables fail due to inconsistency
--
--  Procedure uses another procedure for logging: p_log_msg_data_replication()
--  List of defects is collected and saved at the end of each stage to the local table
--  Example of logging facilities: table, procedure with PRAGMA AUTONOMOUS_TRANSACTIONS in
 --  this repo, file autonomous_db_log.sql
--
--  Connection between databases must exist, either as Oracle DB Link or synonym to another schema
--
--
--  https://github.com/Krzysztof-Rogoz/Oracle-samples
--
--  Created: 6-Feb-2024
--  Author: Krzysztof Rogoz
--  https://www.linkedin.com/in/krzysztof-rogoz-19b6781/
------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE p_replicate_tables
  IS
    TYPE t_table_names IS TABLE OF user_tables.table_name%TYPE
       INDEX BY PLS_INTEGER;       -- associative array
    a_table_con_names t_table_names;
    a_table_cons      t_table_names;
    a_regular_tables  t_table_names;
    TYPE t_log_msgs IS TABLE OF log_data_replication%ROWTYPE;    -- nested table
    a_log_msgs        t_log_msgs := t_log_msgs();                -- initiated as empty collection, any erros will be appended
    --
    sql_dis_constr     VARCHAR2(200);
    sql_enab_constr    VARCHAR2(200);
    sql_drop_partition VARCHAR2(200);
    sql_add_partition  VARCHAR2(200);
    sql_truncate       VARCHAR2(200);
    sql_insert         VARCHAR2(2000);
    --
    v_email_subject   VARCHAR2( 150) := 'Automatic emailing: Oracle data replication - env:' || sys_context('USERENV','DB_NAME') || '-' || USER || ' - result: ';
    v_email_msg       VARCHAR2(4000) := 'The following errors in logs: ' || CHR(10) || CHR(10)|| CHR(10);
    v_tables_count    NUMBER;
    err_num           NUMBER;
    err_msg           VARCHAR2(100);
  BEGIN
    --------------------------------------------------------------
    -- Phase: PRE-replication
    --
    --   Prepare remote tables:
    --     1)  Purge garbage collector
    --     2)  Disable foreign keys
    --     3)  Synchronize partitions BEFORE starting data sync.
    --------------------------------------------------------------
    --
    p_log_msg_data_replication('p_replicate_tables', 'N/A', 'BEGIN', 'Replication started');
    --
    -- (1) to eliminate garbages from partition views in both: local and remote
    EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';
    DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>('PURGE RECYCLEBIN');
    --
    -- (2) disable FK
    SELECT table_name, constraint_name
      BULK COLLECT INTO a_table_con_names, a_table_cons
      FROM user_constraints
     WHERE constraint_type='R'
    ;
    IF  a_table_cons.COUNT > 0 THEN
      FOR numCount IN a_table_cons.FIRST .. a_table_cons.LAST
      LOOP
        BEGIN
           sql_dis_constr:=  'alter table '||a_table_con_names(numCount)||' disable constraint '||a_table_cons(numCount);
           DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>(sql_dis_constr);     -- ***  adjust for another DB / oracle user ***
           EXCEPTION
              WHEN OTHERS THEN
                err_num := SQLCODE;
                err_msg := SUBSTR(SQLERRM, 1, 100);
                a_log_msgs.EXTEND;
                a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
                a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
                a_log_msgs(a_log_msgs.LAST).log_table := a_table_con_names(numCount);
                a_log_msgs(a_log_msgs.LAST).log_level := 'disabling constrains';
                a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
           END;
        END LOOP;
    END IF;
    --
    -- (3a) Drop remote partitions with names not existing in local DB (higher environment)
    FOR rec_partition_to_del IN (SELECT table_name, partition_name
                                   FROM user_tab_partitions@<<dblink_name_or_synonym>> dwn
                                  WHERE partition_name NOT LIKE 'INIT_%'    -- naming convention used, verify this!!!!
                                    AND NOT EXISTS (SELECT 1 FROM user_tab_partitions loc WHERE loc.table_name=dwn.table_name AND loc.partition_name=dwn.partition_name)
                                  ORDER BY table_name, partition_name)
    LOOP
      BEGIN
        sql_drop_partition := 'alter table '|| rec_partition_to_del.table_name ||' drop partition '|| rec_partition_to_del.partition_name;
        DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>(sql_drop_partition);
      EXCEPTION
        WHEN OTHERS THEN
          err_num := SQLCODE;
          err_msg := SUBSTR(SQLERRM, 1, 100);
          a_log_msgs.EXTEND;
          a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
          a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
          a_log_msgs(a_log_msgs.LAST).log_table := rec_partition_to_del.table_name;
          a_log_msgs(a_log_msgs.LAST).log_level := 'Partition to delete: ' || rec_partition_to_del.partition_name;
          a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
      END;
    END LOOP;
    --
    -- (3b) Create remote partitions based on local tables (synchronize higher environment to lower)
    FOR rec_partition_to_add IN (SELECT table_name, partition_name, high_value
                                   FROM user_tab_partitions loc
                                  WHERE partition_name NOT LIKE 'INIT_%'   -- naming convention used, verify this!!!!
                                    AND NOT EXISTS (SELECT 1 FROM user_tab_partitions@<<dblink_name_or_synonym>> dwn
                                                     WHERE loc.table_name=dwn.table_name AND loc.partition_name=dwn.partition_name)
                                  ORDER BY table_name, partition_name)
    LOOP
      BEGIN
        IF INSTR( UPPER(rec_partition_to_add.high_value), 'TO_DATE' )>0 THEN
          sql_add_partition := 'ALTER TABLE '|| rec_partition_to_add.table_name ||' ADD PARTITION '|| rec_partition_to_add.partition_name || ' VALUES LESS THAN ( ' ||rec_partition_to_add.high_value||' )';
        ELSE
          sql_add_partition := 'ALTER TABLE '|| rec_partition_to_add.table_name ||' ADD PARTITION '|| rec_partition_to_add.partition_name || ' VALUES ( ' ||rec_partition_to_add.high_value||' )';
        END IF;
        DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>(sql_add_partition);
      EXCEPTION
        WHEN OTHERS THEN
          err_num := SQLCODE;
          err_msg := SUBSTR(SQLERRM, 1, 100);
          a_log_msgs.EXTEND;
          a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
          a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
          a_log_msgs(a_log_msgs.LAST).log_table := rec_partition_to_add.table_name;
          a_log_msgs(a_log_msgs.LAST).log_level := 'Partition to delete: ' || rec_partition_to_add.partition_name;
          a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
      END;
    END LOOP;
    --------------------------------------------------------------
    -- Phase: REPLICATION
    --
    --   After partitions are in sync now, both: partitioned and non-partitioned tables
    --     are refreshed in the same way : Truncate remote tables AND insert current content of local tables to remote
    --   *) Global temporary tables and LOG_tables excluded from replication (they would be always empty when querying from another session :) )

    --   Only Materialized Views are synchronized in a different way: force refresh on remote MV's
    --------------------------------------------------------------
    --
    -- *** the main query to extract BOTH: regular AND partitioned tables ***
    SELECT ut.table_name
      BULK COLLECT INTO a_regular_tables
      FROM user_tables ut
     WHERE temporary = 'N'                -- exclude oracle global temporary tables from replication
       AND table_name NOT LIKE 'LOG_%'    -- exclude LOGs,  naming convention used, verify this!!!!
       AND NOT EXISTS (SELECT 1 FROM user_mviews mv WHERE ut.table_name=mv.mview_name)          -- exclude MV's from this step
    ;
    FOR indx IN  1 .. a_regular_tables.COUNT
    LOOP
      BEGIN
        sql_truncate := 'truncate table ' || a_regular_tables(indx);  -- will be executed on REMOTE DB !!!
        sql_insert := 'insert into '|| a_regular_tables(indx) ||'@<<dblink_name_or_synonym>> select * from '|| a_regular_tables(indx);
        DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>(sql_truncate);  -- ***  adjust for another DB / oracle user ***
        EXECUTE IMMEDIATE  sql_insert;
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          err_num := SQLCODE;
          err_msg := SUBSTR(SQLERRM, 1, 100);
          a_log_msgs.EXTEND;
          a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
          a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
          a_log_msgs(a_log_msgs.LAST).log_table :=  a_regular_tables(indx);
          a_log_msgs(a_log_msgs.LAST).log_level :='truncate / insert together';
          a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
      END;
    END LOOP;
    v_tables_count := a_regular_tables.COUNT;
    --
    -- find all MV's and enforce refresh
    FOR rec_mviews IN (SELECT mview_name FROM user_mviews mv WHERE refresh_mode='DEMAND')
    LOOP
      BEGIN
        DBMS_MVIEW.REFRESH@<<dblink_name_or_synonym>>(rec_mviews.mview_name);
      EXCEPTION
        WHEN OTHERS THEN
          err_num := SQLCODE;
          err_msg := SUBSTR(SQLERRM, 1, 100);
          a_log_msgs.EXTEND;
          a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
          a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
          a_log_msgs(a_log_msgs.LAST).log_table :=  rec_mviews.mview_name;
          a_log_msgs(a_log_msgs.LAST).log_level :='refresh MV';
          a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
      END;
    END LOOP;
    --
    --------------------------------------------------------------
    -- Phase: POST-replication
    --
    --   Enable foreign keys, env-specific data updates, sent result email
    --------------------------------------------------------------
    IF  a_table_cons.COUNT > 0 THEN
      FOR numCount IN a_table_cons.FIRST .. a_table_cons.LAST
      LOOP
        BEGIN
          sql_enab_constr:=  'alter table '||a_table_con_names(numCount)||' enable constraint '||a_table_cons(numCount);
          DBMS_UTILITY.EXEC_DDL_STATEMENT@<<dblink_name_or_synonym>>(sql_enab_constr);
        EXCEPTION
          WHEN OTHERS THEN
            err_num := SQLCODE;
            err_msg := SUBSTR(SQLERRM, 1, 100);
          a_log_msgs.EXTEND;
          a_log_msgs(a_log_msgs.LAST).log_timestamp := SYSDATE;
          a_log_msgs(a_log_msgs.LAST).log_procedure_name := 'p_replicate_tables';
          a_log_msgs(a_log_msgs.LAST).log_table := a_table_con_names(numCount);
          a_log_msgs(a_log_msgs.LAST).log_level :='enable constrains';
          a_log_msgs(a_log_msgs.LAST).log_msg := 'error number: '||err_num || CHR(10)|| 'err message: '||err_msg;
        END;
      END LOOP;
    END IF;
    --
    -- Prepare email text: consolidate messages from log table into summary
    v_email_msg := TO_CHAR(v_tables_count) || ' tables refreshed, ' || CHR(10)  || v_email_msg;
    IF a_log_msgs.COUNT=0 THEN
      v_email_subject :=  v_email_subject || ' FULL SUCCESS!';
      v_email_msg := v_email_msg || CHR(10)|| CHR(10)|| '    *** No errors detected. All tables, partitioned tables, MVs succesfully replicated ***';
    ELSE
      FOR l IN 1 .. a_log_msgs.COUNT
      LOOP
        IF l>=12 THEN  EXIT; END IF;
        v_email_msg := v_email_msg || CHR(10)|| 'Table: '|| a_log_msgs(l).log_table|| CHR(10)||'level of procedure: '|| a_log_msgs(l).log_level|| CHR(10)|| a_log_msgs(l).log_msg|| CHR(10)|| '  ***';
      END LOOP;
      v_email_subject :=  v_email_subject || ' COMPLETED, ' || TO_CHAR(a_log_msgs.COUNT) || ' tables failed...';
    END IF;
    -- Send email to responsible person/group
    ---p_send_email(..v_email_msg..);  ..if applicable
    --
    -- Save error messages permanently - into Log Table
    FOR l IN 1 .. a_log_msgs.COUNT
    LOOP
      INSERT INTO log_data_replication
        VALUES a_log_msgs(l);
    END LOOP;
    COMMIT;
    p_log_msg_data_replication('p_replicate_tables', 'N/A', 'END', 'Replication ended');
EXCEPTION
  WHEN OTHERS THEN
    v_email_msg := SUBSTR( SQLERRM || CHR(13) || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ,0,1999);
    p_log_msg_data_replication('p_replicate_tables', 'WHEN OTHERS EXCEPTION!!!', 'unknown', v_email_msg);
    ---p_send_email(..v_email_msg..);  ..if applicable
END p_replicate_tables;
/