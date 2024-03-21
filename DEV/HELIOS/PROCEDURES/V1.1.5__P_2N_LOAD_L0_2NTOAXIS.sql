CREATE OR REPLACE PROCEDURE PUBLIC.P_2N_LOAD_L0_2NTOAXIS()
RETURNS NUMBER(38,0)
LANGUAGE SQL
COMMENT='user-defined procedure'
EXECUTE AS CALLER
AS 'DECLARE row_count integer default 0;
BEGIN 

use L0_HELIOS.public;
ALTER SESSION SET TIMEZONE = ''Europe/Prague'';
create or replace TEMPORARY table JSON_temp_table (json_data  variant );

DECLARE SQL string;


BEGIN 
    SELECT 
        CONCAT(''COPY INTO JSON_temp_table (json_data) FROM '',CONCAT(''@HELIOS_JSON_INT_STAGE/2NtoAxis_'',CAST(CAST(CURRENT_DATE as DATE) as varchar),''.json.gz''),'' on_error = ''''skip_file'''';'')
    INTO :SQL;
    EXECUTE IMMEDIATE :SQL;
END ;

TRUNCATE L0_HELIOS.PUBLIC.L0_2N_TO_AXIS_ART_NUM;

INSERT INTO L0_HELIOS.PUBLIC.L0_2N_TO_AXIS_ART_NUM
SELECT 
   j.value:article_id::int as "article_id",
   j.value:article_code::varchar as "article_code",
   j.value:article_name::varchar as "article_name",
   j.value:article_name_en::varchar as "article_name_en",
   j.value:axis_article_code::varchar as "axis_article_code",
   j.value:axis_article_name::varchar as "axis_article_name",
   j.value:Product_sales_group_id::int as "Product_sales_group_id",
   j.value:Product_sales_group_name::varchar as "Product_sales_group_name",
   j.value:Product_sales_cross_group_id::int as "Product_sales_cross_group_id",
   j.value:Product_Sales_Cross_Group_name_cz::varchar as "Product_Sales_Cross_Group_name_cz",
   j.value:Product_Sales_Cross_Group_name_en::varchar as "Product_Sales_Cross_Group_name_en"   
FROM json_temp_table, table(flatten(JSON_DATA:Translate)) as j

;
return :sqlrowcount; /*POČET VLOŽENÝCH ZÁZNAMŮ*/
end';