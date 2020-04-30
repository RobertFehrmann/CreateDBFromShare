create schema if not exists metadata;
CREATE OR REPLACE PROCEDURE METADATA.SP_COPY_FROM_SHARED_DB(i_src_db string, i_src_schema string, i_tgt_db string, i_tgt_schema string)
  RETURNS ARRAY
  LANGUAGE javascript
  EXECUTE AS caller
AS
$$
//note:  this proc returns an array, either success or fail right now
try {

    var src_db  = I_SRC_DB;
    var tgt_db  = I_TGT_DB;
    var src_schema  = I_SRC_SCHEMA;
    var tgt_schema  = I_TGT_SCHEMA;

    var meta_schema = "METADATA";
    var tmp_schema  = "TMP";

    var whereAmI = 1;
    var return_array = [];
    var table_name_array = [];
    var counter = 0;
    var table_name = "TBD";
    var alterSqlQuery = "TBD";
    var status="END"

    var procName = Object.keys(this)[0];

    function log ( msg ) {
       return_array.push(msg)
    }

    function flush_log (status){
       var message="";
       for (i=0; i < return_array.length; i++) {
          message=message+String.fromCharCode(13)+return_array[i]
       }

       var sqlquery = "INSERT INTO metadata.log (status,message) values ('" + status + "','" + message + "');";
       snowflake.execute({sqlText: sqlquery});
    }

    log("procName: "+procName+" BEGIN")
    flush_log("START")

    whereAmI = 2;
    var sqlquery = "CREATE SCHEMA IF NOT EXISTS " + tgt_db + "." + tgt_schema + ";";
    snowflake.execute({sqlText: sqlquery});
    var sqlquery = "CREATE SCHEMA IF NOT EXISTS " + tgt_db + "." + tmp_schema + ";";
    snowflake.execute({sqlText: sqlquery});

    whereAmI = 3;
    var sqlquery = "SHOW TABLES IN SCHEMA " + src_db + "." + src_schema + ";";
    snowflake.execute({sqlText: sqlquery});

    whereAmI = 4;
    sqlquery = "SELECT \"name\" as table_name FROM (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));";
    var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    whereAmI = 5;
    while (tableNameResultSet.next())  {
       counter = counter + 1;
       table_name = tableNameResultSet.getColumnValue(1);
       table_name_array.push(table_name);

       //build tmp tables for schema check
       //I am pretty sure we do not need the tmp schema
       sqlquery = "CREATE OR REPLACE TEMPORARY TABLE " + tgt_db + "." + tmp_schema + "." + table_name;
       sqlquery = sqlquery + " AS SELECT * FROM " + src_db + "." + src_schema + "." + table_name + " WHERE 1=0;";
       snowflake.execute({sqlText: sqlquery});

       //build new tables in target if not there
       sqlquery = "CREATE TABLE IF NOT EXISTS " + tgt_db + "." + tgt_schema + "." + table_name;
       sqlquery = sqlquery + " AS SELECT * FROM " + src_db + "." + src_schema + "." + table_name + " WHERE 1=0;";
       snowflake.execute({sqlText: sqlquery});

       log("table_name(" + counter.toString() + "): " + table_name);
       }

    whereAmI = 6;
    log("end create tables loop - counter: " + counter);
    counter=0;

    whereAmI = 7;
    //add new columns to target tables
    var sqlquery = `
        SELECT  a.table_name,
                a.column_name,
                a.data_type,
                a.ordinal_position
        FROM    information_schema.columns a
        WHERE   a.table_catalog = '` + tgt_db + `'
        AND     a.table_schema = '` + tmp_schema + `'
        AND NOT EXISTS (
            SELECT *
            FROM   information_schema.columns b
            WHERE  b.table_catalog = '` + tgt_db + `'
            AND    b.table_schema = '` + tgt_schema + `'
            AND    a.table_name = b.table_name
            AND    a.column_name = b.column_name)
        ORDER BY a.table_name, a.ordinal_position;`;

    whereAmI = 8;
    var alterColumnList = snowflake.execute({sqlText: sqlquery});
    while (alterColumnList.next()){
        alterSqlQuery = "ALTER TABLE " + tgt_db + "." + tgt_schema + "." + alterColumnList.getColumnValue(1);
        alterSqlQuery = alterSqlQuery + " ADD COLUMN \"" + alterColumnList.getColumnValue(2) + "\" " + alterColumnList.getColumnValue(3) + ";";
        snowflake.execute({sqlText: alterSqlQuery});
        log("ALTER COMMAND: " + alterSqlQuery);
    }

    whereAmI = 9;
    //refresh data transactionally consistent. Delete all existing rows (truncate doesn't work because it's DDL)
    //then insert all new rows; column sequence could be different, therefor list column names
    for (i=0; i < table_name_array.length; i++) {
        try {

            table_name = table_name_array[i];

            sqlquery = "BEGIN;";
            snowflake.execute({sqlText: sqlquery});

            sqlquery = "DELETE FROM " + tgt_db + "." + tgt_schema + "." + table_name + ";";
            snowflake.execute({sqlText: sqlquery});

            sqlquery = `
                SELECT listagg('"'||column_name||'"',',')
                FROM information_schema.columns a
                WHERE   a.table_catalog = '` + tgt_db + `'
                AND     a.table_schema = '` + tmp_schema + `'
                AND     a.table_name = '` + table_name + `'
                ORDER BY a.ordinal_position;`
            var selectColumnList=snowflake.execute({sqlText: sqlquery});

            if (selectColumnList.next()) {
               sqlquery = "INSERT INTO " + tgt_db + "." + tgt_schema + "." + table_name + " (" + selectColumnList.getColumnValue(1) + ") ";
               sqlquery = sqlquery + " SELECT " + selectColumnList.getColumnValue(1) + " FROM " + src_db + "." + src_schema + "." + table_name + ";";
               snowflake.execute({sqlText: sqlquery});

               sqlquery = "COMMIT;";
               snowflake.execute({sqlText: sqlquery});
               log("INSERT INTO SELECT.... : " + table_name + "; COMPLETE")
             } else {
               sqlquery = "ROLLBACK;";
               snowflake.execute({sqlText: sqlquery});
               log("NO COLUMN LIST FOUND FOR TABLE " + table_name + "; ROLLBACK")
               status="ERROR"
             }
        }
        catch (err) {
            var sqlquery = "ROLLBACK;";
            snowflake.execute({sqlText: sqlquery});
            log("ERROR found - ROLLBACK - for table " + table_name);
            log("whereAmI: " + whereAmI);
            log("err.code: " + err.code);
            log("err.state: " + err.state);
            log("err.message: " + (err.message).replace(/'/g,""));
            log("err.stacktracetxt: " + err.stacktracetxt);
            log("end error for table " + table_name);
            log("drop table " + table_name + " and restart the load");
            status="ERROR"
        }
    }

    whereAmI = 10;
    var sqlquery = `
        SELECT  a.table_name,
                a.column_name,
                a.ordinal_position
        FROM    information_schema.columns a
        WHERE   a.table_catalog = '` + tgt_db + `'
        AND     a.table_schema = '` + tgt_schema + `'
        AND NOT EXISTS (
            SELECT *
            FROM   information_schema.columns b
            WHERE  b.table_catalog = '` + tgt_db + `'
            AND    b.table_schema = '` + tmp_schema + `'
            AND    a.table_name = b.table_name
            AND    a.column_name = b.column_name)
        ORDER BY a.table_name, a.ordinal_position;`;

    whereAmI = 11;
    var alterColumnList = snowflake.execute({sqlText: sqlquery});
    while (alterColumnList.next()){
        alterSqlQuery = "ALTER TABLE " + tgt_db + "." + tgt_schema + "." + alterColumnList.getColumnValue(1);
        alterSqlQuery = alterSqlQuery + " DROP COLUMN \"" + alterColumnList.getColumnValue(2) + "\";" ;
        snowflake.execute({sqlText: alterSqlQuery});
        log("ALTER COMMAND: " + alterSqlQuery);
    }

    log("procName: " + procName + " END " + status)
    flush_log(status)

    return return_array
    }

catch (err) {
    log("ERROR found - MAIN try command");
    log("whereAmI: " + whereAmI);
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + (err.message).replace(/'/g,""));
    log("err.stacktracetxt: " + err.stacktracetxt);
    log("procName: " + procName + " END / FAILURE")

    flush_log("ERROR")

    return return_array;
}
$$
;
