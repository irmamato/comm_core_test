
select distinct campaign_id
    --, split_part(labels, ',', 2) as campaign_name
    , concat(cast(campaign_id as string), ' campaign name') as campaign_name
from {{ ref("cleansed_google__ads_spend") }}
order by campaign_id