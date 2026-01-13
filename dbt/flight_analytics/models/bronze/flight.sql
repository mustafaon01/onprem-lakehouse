select
  flight_id,
  flight_number,
  departure,
  destination,
  departure_time,
  arrival_time,
  airplane_id
from {{ source('airline', 'flight') }}