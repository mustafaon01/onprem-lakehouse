select
  reservation_id,
  passenger_name,
  passenger_email,
  reservation_code,
  flight_id,
  status,
  created_at
from {{ source('airline', 'reservation') }}