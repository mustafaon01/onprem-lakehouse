select
  airplane_id,
  tail_number,
  model,
  capacity,
  production_year,
  status
from {{ source('airline', 'airplane') }}