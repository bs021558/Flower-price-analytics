CREATE SERVER {{ params.server_name}}
FOREIGN DATA WRAPPER {{ params.extension_name }} OPTIONS
(
    HOST '{{ params.storage_endpoint }}',
    BUCKET '{{ params.bucket_name }}',
    ID '{{ params.key_id }}',
    KEY '{{ params.key_secret}}'
);