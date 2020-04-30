create or replace table metadata.log (
   id integer AUTOINCREMENT (0,1)
   ,create_ts timestamp_ltz default current_timestamp
   , session_id number default to_number(current_session())
   ,status varchar
   ,message varchar);
