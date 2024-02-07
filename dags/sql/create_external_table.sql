CREATE FOREIGN TABLE {{ params.table_name }}
({{ params.columns }}) 
SERVER {{ params.server_name }} 
OPTIONS (
    FILEPATH '{{ params.file_path }}', 
    DELIMITER ',' ,
    FORMAT 'csv', 
    ENCODING 'utf8', 
    PARSE_ERRORS '100',
    {{ params.header }}
);