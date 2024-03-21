CREATE OR REPLACE PROCEDURE PUBLIC.P_2N_LOAD_L0_ACCOUNT_STATEMENTS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE row_count integer default 0;
BEGIN 

use L0_HELIOS.public;
ALTER SESSION SET TIMEZONE = ''Europe/Prague'';
create or replace TEMPORARY table JSON_temp_table (json_data  variant );

DECLARE SQL string;


BEGIN 
    SELECT 
        CONCAT(''COPY INTO JSON_temp_table (json_data) FROM '',CONCAT(''@HELIOS_JSON_INT_STAGE/AccStmnts_'',CAST(CAST(CURRENT_DATE as DATE) as varchar),''.json.gz''),'' on_error = ''''skip_file'''';'')
    INTO :SQL;
    EXECUTE IMMEDIATE :SQL;
END ;




TRUNCATE TABLE L0_HELIOS.PUBLIC.L0_2N_ACCOUNT_STATEMENTS;

INSERT INTO L0_HELIOS.PUBLIC.L0_2N_ACCOUNT_STATEMENTS
SELECT
    j.value:account_2n_id::int as "account_2n_id",
    j.value:account_2n_code::varchar(255) as "account_2n_code",
    j.value:account_2n_name::varchar(255) as "account_2n_name",
    j.value:Axis_AARO_Account_id::int as "Axis_AARO_Account_id",
    j.value:Axis_AARO_Account_code::varchar(255) as "Axis_AARO_Account_code",
    j.value:Axis_AARO_Account_cz_name::varchar(255) as "Axis_AARO_Account_cz_name",
    j.value:Axis_AARO_Account_en_name::varchar(255) as "Axis_AARO_Account_en_name",
    j.value:Axis_AARO_Account_group_id::int as "Axis_AARO_Account_group_id",
    j.value:Axis_AARO_Account_group_code::varchar(255) as "Axis_AARO_Account_group_code",
    j.value:Axis_AARO_Account_group_cz_name::varchar(255) as "Axis_AARO_Account_group_cz_name",
    j.value:Axis_AARO_Account_group_en_name::varchar(255) as "Axis_AARO_Account_group_en_name",
    j.value:Axis_AARO_Account_family_id::int as "Axis_AARO_Account_family_id",
    j.value:Axis_AARO_Account_family_code::varchar(255) as "Axis_AARO_Account_family_code",
    j.value:Axis_AARO_Account_family_name::varchar(255) as "Axis_AARO_Account_family_name",
    j.value:cost_type::varchar(50) as "cost_type"
FROM json_temp_table, table(flatten(JSON_DATA, ''AS'')) as j
;
LET vystup := :sqlrowcount;

return vystup; /*POČET VLOŽENÝCH ZÁZNAMŮ*/
end';