select count(*) as country_count
from {{ ref('dim_country') }}
having count(*) != 54
