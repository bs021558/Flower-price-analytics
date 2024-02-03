CREATE SERVER {{ server_name}}
FOREIGN DATA WRAPPER {{ extension_name }} OPTIONS
(
    HOST '{{ storage_endpoint }}',
    BUCKET '{{ bucket_name }}',
    ID '{{ key_id }}',
    KEY '{{ key_secret}}'
);