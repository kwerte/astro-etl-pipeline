{{ config(
    materialized='incremental',
    unique_key='voter_id',
    incremental_strategy='merge') }}

select *
from {{ ref('stg_voter_data_all') }}
where voter_id not in (select voter_id from {{ ref('stg_voter_data_errors') }})

{% if is_incremental() %}
{% endif %}