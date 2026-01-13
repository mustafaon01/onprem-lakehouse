select
  flight_id,
  trim(flight_number) as flight_number,
  trim(departure) as departure,
  trim(destination) as destination,
  departure_time,
  arrival_time,
  airplane_id
from {{ ref('flight') }}