------------------------------------------------------------------------------
--
--  Autonomous DB logging in local table
--
--   Oracle PRAGMA AUTONOMOUS_TRANSACTION
--   enables logging transaction independent from the main transaction
--   Even if Oracle rollbacks session, log record remains in local table
--
--   USAGE example:
--     p_log_msg('ERR','name or sql code', SQLCODE||  SUBSTR(SQLERRM, 1, 500) );
--
--
--  https://github.com/Krzysztof-Rogoz/Oracle-samples
--
--  Created: 6-Feb-2024
--  Author: Krzysztof Rogoz
--  https://www.linkedin.com/in/krzysztof-rogoz-19b6781/
------------------------------------------------------------------------------

--
-- Create log table and procedure
CREATE TABLE log_dataload_msgs (
          log_timestamp timestamp,
          log_level VARCHAR2(4),         --ERR, WARN, INFO
          log_module VARCHAR2(100),
          log_msg VARCHAR2(500),
          CONSTRAINT chk_log_level CHECK (log_level IN ('ERR', 'WARN', 'INFO')) ENABLE
);

 CREATE OR REPLACE PROCEDURE p_log_msg (p_level VARCHAR2, p_module_name VARCHAR2, p_msg VARCHAR2)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO log_dataload_msgs (
          log_timestamp,
          log_level,         --ERR, WARN, INFO
          log_module,
          log_msg)
       VALUES
          (SYSDATE, p_level, p_module_name, p_msg);
       COMMIT;
   END;

--
-- Test case
DECLARE
  log_record  log_dataload_msgs%ROWTYPE;
  my_sql   VARCHAR2(500):= 'CREATE TABLE foo (id INTEGER, bad_col my_foo_type);';
BEGIN
  -- attempt to execute DDL
  BEGIN
    EXECUTE IMMEDIATE my_sql;
  EXCEPTION
    WHEN OTHERS THEN
      p_log_msg('ERR',my_sql,SQLCODE||  SUBSTR(SQLERRM, 1, 200) );
   END;
   --
   -- check result, find the last log record:
   SELECT * INTO log_record FROM log_dataload_msgs log
     WHERE NOT EXISTS (SELECT 1 FROM log_dataload_msgs older WHERE older.log_timestamp > log.log_timestamp);
   DBMS_OUTPUT.PUT_LINE('After attempt, logged in local table: ' ||chr(10)||chr(13)||'Command:'|| log_record.log_module);
   DBMS_OUTPUT.PUT_LINE('Result: '||log_record.log_msg);
 END;
