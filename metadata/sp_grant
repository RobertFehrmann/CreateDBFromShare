CREATE OR REPLACE PROCEDURE METADATA.SP_GRANT(i_tgt_db string, i_tgt_schema string, i_share string)
  RETURNS ARRAY
  LANGUAGE javascript
  EXECUTE AS caller
AS
$$
//note:  this proc returns an array, either success or fail right now
try {

    var tgt_db  = I_TGT_DB;
    var tgt_schema  = I_TGT_SCHEMA;
    var share = I_SHARE;

    var meta_schema = "METADATA";
    var tmp_schema  = "TMP";

    var whereAmI = 1;
    var return_array = [];
    var status = "END";

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
    //flush_log("START")

    whereAmI = 3;
    snowflake.execute({sqlText: "GRANT USAGE ON DATABASE " + tgt_db + " TO SHARE " + share + ";"});
    snowflake.execute({sqlText: "GRANT USAGE ON SCHEMA " + tgt_db + "." + tgt_schema + " TO SHARE " + share + ";"});
    snowflake.execute({sqlText: "GRANT SELECT ON ALL TABLES IN SCHEMA " + tgt_db + "." + tgt_schema + " TO SHARE " + share + ";"});

    log("procName: " + procName + " END " + status)
    //flush_log(status)

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

    //flush_log("ERROR")

    return return_array;
}
$$
;
