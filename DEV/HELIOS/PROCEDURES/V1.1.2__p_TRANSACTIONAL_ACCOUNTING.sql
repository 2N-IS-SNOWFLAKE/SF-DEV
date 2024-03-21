/*
TVORBA PROCEDURY PRO NAPLNĚNÍ TRANSACTIONAL ACCOUNTING
*/
CREATE OR REPLACE PROCEDURE PUBLIC.P_2N_LOAD_L0_TRANSACTIONAL_ACCOUNTING()
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS CALLER
AS 
DECLARE row_count integer default 0;
BEGIN 
ALTER SESSION SET TIMEZONE = 'Europe/Prague';
CREATE OR REPLACE TEMPORARY table JSON_temp_table (json_data  variant );

DECLARE SQL string;


BEGIN 
    SELECT 
        CONCAT('COPY INTO JSON_temp_table (json_data) FROM ',CONCAT('@HELIOS_JSON_INT_STAGE/TransAcc_',CAST(CAST(CURRENT_DATE as DATE) as varchar),'.json.gz'),' on_error = ''skip_file'';')
    INTO :SQL;
    EXECUTE IMMEDIATE :SQL;
END ;

CREATE OR REPLACE TEMPORARY TABLE PJTEMP AS
SELECT
	j.value:cislo_subjektu::int as "cislo_subjektu"
    ,j.value:cislo_objektu::int as "cislo_objektu"
	,j.value:transaction_number::string as "transaction_number"
	,j.value:line_description::string as "line_description"
	,j.value:pocetkc::numeric(32,10) as "pocet_kc"
	,j.value:kod_meny::varchar as "kod_meny"
	,j.value:pocetmeny::numeric(32,10) as "pocetmeny"
	,j.value:pripad::timestamp_ntz as "datum_pripadu"
	,j.value:transaction_cred_deb::varchar as "transaction_cred_deb"
	,j.value:account::int as "account"
	,j.value:axis_account::varchar as "axis_account"
	,j.value:datum_kurzu::timestamp_ntz as "datum_kurzu"
	,j.value:duzp::timestamp_ntz as "duzp"
	,j.value:poznamka::varchar as "poznamka"
	,j.value:pocetmj::numeric(32,10) as "pocetmj"
	,j.value:primary_document_reference::varchar as "primary_document_reference"
	,j.value:party_id::int as "party_id"
	,j.value:party_name::varchar as "party_name"
	,j.value:department_id::int as "department_id"
	,j.value:department_code::varchar as "department_code"
	,j.value:axis_aaro_reference::varchar as "axis_aaro_reference"
	,j.value:contract_ID::int as "contract_ID"
	,j.value:primary_document_ID::int as "primary_document_ID"
	,j.value:created_by::varchar as "created_by"
	,j.value:legal_entity_code::varchar as "legal_entity_code"
	,j.value:ufd_externista::varchar as "ufd_externista"
	,j.value:group_axis::int as "group_axis"
FROM json_temp_table, table(flatten(JSON_DATA:TransArc)) as j;


MERGE INTO PUBLIC.L0_2N_TRANSACTIONAL_ACCOUNTING USING PJTEMP ON "CISLO_SUBJEKTU" = PJTEMP."cislo_subjektu" AND "CISLO_OBJEKTU" = PJTEMP."cislo_objektu"
WHEN MATCHED THEN UPDATE 
    SET 
		"CISLO_SUBJEKTU"				=				PJTEMP."cislo_subjektu",
		"CISLO_OBJEKTU"					= 				PJTEMP."cislo_objektu",
		"TRANSACTION_NUMBER"			=				PJTEMP."transaction_number",
		"LINE_DESCRIPTION"				=				PJTEMP."line_description",
		"POCETKC"						=				PJTEMP."pocet_kc",
		"KOD_MENY"						=				PJTEMP."kod_meny",
		"POCETMENY"						=				PJTEMP."pocetmeny",
		"PRIPAD"						=				PJTEMP."datum_pripadu",
		"TRANSACTION_CRED_DEB"			=				PJTEMP."transaction_cred_deb",
		"ACCOUNT"						=				PJTEMP."account",
		"AXIS_ACCOUNT"					=				PJTEMP."axis_account",
		"DATUM_KURZU"					=				PJTEMP."datum_kurzu",
		"DUZP"							=				PJTEMP."duzp",
		"POZNAMKA"						=				PJTEMP."poznamka",
		"POCETMJ"						=				PJTEMP."pocetmj",
		"PRIMARY_DOCUMENT_REFERENCE"	=				PJTEMP."primary_document_reference",
		"PARTY_ID"						=				PJTEMP."party_id",
		"PARTY_NAME"					=				PJTEMP."party_name",
		"DEPARTMENT_ID"					=				PJTEMP."department_id",
		"DEPARTMENT_REFERENCE"			=				PJTEMP."department_code",
		"AXIS_AARO_REFERENCE"			=				PJTEMP."axis_aaro_reference",
		"CONTRACT_ID"					=				PJTEMP."contract_ID",
		"PRIMARY_DOCUMENT_ID"			=				PJTEMP."primary_document_ID",
		"CREATED_BY"					=				PJTEMP."created_by",
		"LEGAL_ENTITY_CODE"				=				PJTEMP."legal_entity_code",
		"UDF_EXTERNISTA"				=				PJTEMP."ufd_externista",
		"GROUP_AXIS"					=				PJTEMP."group_axis"
WHEN NOT MATCHED THEN INSERT 
(
	"CISLO_SUBJEKTU",
	"CISLO_OBJEKTU",
	"TRANSACTION_NUMBER",
	"LINE_DESCRIPTION",
	"POCETKC",
	"KOD_MENY",
	"POCETMENY",
	"PRIPAD",
	"TRANSACTION_CRED_DEB",
	"ACCOUNT",
	"AXIS_ACCOUNT",
	"DATUM_KURZU",
	"DUZP",
	"POZNAMKA",
	"POCETMJ",
	"PRIMARY_DOCUMENT_REFERENCE",
	"PARTY_ID",
	"PARTY_NAME",
	"DEPARTMENT_ID",
	"DEPARTMENT_REFERENCE",
	"AXIS_AARO_REFERENCE",
	"CONTRACT_ID",
	"PRIMARY_DOCUMENT_ID",
	"CREATED_BY",
	"LEGAL_ENTITY_CODE",
	"UDF_EXTERNISTA",
	"GROUP_AXIS"
)
VALUES 
(
	PJTEMP."cislo_subjektu",
	PJTEMP."cislo_objektu",
	PJTEMP."transaction_number",
	PJTEMP."line_description",
	PJTEMP."pocet_kc",
	PJTEMP."kod_meny",
	PJTEMP."pocetmeny",
	PJTEMP."datum_pripadu",
	PJTEMP."transaction_cred_deb",
	PJTEMP."account",
	PJTEMP."axis_account",
	PJTEMP."datum_kurzu",
	PJTEMP."duzp",
	PJTEMP."poznamka",
	PJTEMP."pocetmj",
	PJTEMP."primary_document_reference",
	PJTEMP."party_id",
	PJTEMP."party_name",
	PJTEMP."department_id",
	PJTEMP."department_code",
	PJTEMP."axis_aaro_reference",
	PJTEMP."contract_ID",
	PJTEMP."primary_document_ID",
	PJTEMP."created_by",
	PJTEMP."legal_entity_code",
	PJTEMP."ufd_externista",
	PJTEMP."group_axis"
);

return :sqlrowcount; /*POČET VLOŽENÝCH ZÁZNAMŮ*/
end;
;