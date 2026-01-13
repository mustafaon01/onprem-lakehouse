select
  reservation_id,
  trim(passenger_name) as passenger_name,
  lower(trim(passenger_email)) as passenger_email,
  trim(reservation_code) as reservation_code,
  flight_id,
  cast(status as boolean) as is_confirmed,
  created_at
from {{ ref('reservation') }}