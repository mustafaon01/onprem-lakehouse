select
  f.flight_id,
  f.flight_number,
  f.departure,
  f.destination,
  f.departure_time,
  f.arrival_time,
  a.airplane_id,
  a.tail_number,
  a.airplane_model,
  count(r.reservation_id) as reservation_count,
  count_if(r.is_confirmed) as confirmed_reservation_count,
  max(r.created_at) as last_reservation_at
from {{ ref('stg_flight') }} f
left join {{ ref('stg_airplane') }} a
  on f.airplane_id = a.airplane_id
left join {{ ref('stg_reservation') }} r
  on f.flight_id = r.flight_id
group by
  f.flight_id,
  f.flight_number,
  f.departure,
  f.destination,
  f.departure_time,
  f.arrival_time,
  a.airplane_id,
  a.tail_number,
  a.airplane_model