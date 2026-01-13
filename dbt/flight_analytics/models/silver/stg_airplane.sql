select
  airplane_id,
  trim(tail_number) as tail_number,
  trim(model) as airplane_model,
  cast(capacity as integer) as capacity,
  cast(production_year as integer) as production_year,
  cast(status as boolean) as is_active
from {{ ref('airplane') }}