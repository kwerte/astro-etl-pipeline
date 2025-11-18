{{ config(materialized='table') }}

with validated as (
    select
        *,
        -- Create a flag for each failure reason
        case when age < 18 then 'Invalid Age' else null end as error_age,
        case when email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' then 'Invalid Email' else null end as error_email,
        case when state_code not in ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') then 'Invalid State' else null end as error_state
    from {{ ref('stg_voter_data_all') }}
)

select *
from validated
where
    error_age is not null
    or error_email is not null
    or error_state is not null