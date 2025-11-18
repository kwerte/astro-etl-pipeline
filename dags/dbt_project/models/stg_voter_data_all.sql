-- This model will be created as a TABLE in your 'dbt_output' schema
{{
  config(
    materialized='ephemeral'
  )
}}

select
    -- Rename, trim whitespace, and standardize case
    "id" as voter_id,
    trim("first_name") as first_name,
    trim("last_name") as last_name,
    
    -- Cast data types
    "age"::integer as age,
    "gender" as gender,
    upper(trim("state")) as state_code,
    initcap(trim("party")) as political_party, -- 'Independent'
    lower(trim("email")) as email,
    "registered_date"::date as registered_at_date,
    "last_voted_date"::date as last_voted_at_date,
    
    -- Standardize timestamp with a specific format
    -- 'FMMM/FMDD/YYYY...' handles non-zero-padded dates like '2/8/2024'
    to_timestamp("updated_at", 'FMMM/FMDD/YYYY HH24:MI:SS') as updated_at_timestamp

from {{ source('raw_postgres', 'raw_voter_data') }}