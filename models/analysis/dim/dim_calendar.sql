
select distinct cast(datetime_in_UTC as date) as date_in_UTC
from {{ ref("cleansed_crm__orders")}}
order by 1