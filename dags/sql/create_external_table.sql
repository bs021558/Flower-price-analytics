CREATE FOREIGN TABLE {{ table_name }}
(...) 
SERVER {{ server_name }} 
OPTIONS (
    FILEPATH '{{ file_path }}', 
    DELIMITER ',' ,
    FORMAT 'csv', 
    ENCODING 'utf8', 
    PARSE_ERRORS '100'
);