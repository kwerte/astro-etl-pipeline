{{
  config(
    materialized='table'
  )
}}

-- Calculate key demographic and engagement metrics
with state_party_aggregates as (
    select
        state_code,
        political_party,
        count(voter_id) as registered_in_party,
        avg(age) as average_voter_age,
        min(registered_at_date) as earliest_registration_date,
        max(last_voted_at_date) as most_recent_vote_date,
        avg({{ datediff("last_voted_at_date", "current_date()", "day") }}) as avg_days_since_last_vote,
        COUNT(CASE WHEN last_voted_at_date < CURRENT_DATE() - INTERVAL '4 years' THEN 1 END) as churn_risk_count
    from {{ ref('stg_clean_voter_data') }}
    
    group by 1,2 
)

final_mart as (
    select
        *,
        sum(registered_in_party) over (partition by state_code) as total_voters_in_state,
        (registered_in_party::float / sum(registered_in_party) over (partition by state_code)) as party_concentration_index
        
    from state_party_aggregates
)

select *
from final_mart