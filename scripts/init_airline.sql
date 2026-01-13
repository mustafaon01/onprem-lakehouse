-- =====================================================
-- Schema
-- =====================================================
CREATE SCHEMA IF NOT EXISTS iceberg.airline;
USE iceberg.airline;

-- =====================================================
-- Tables
-- =====================================================
CREATE TABLE IF NOT EXISTS airplane (
    airplane_id BIGINT,
    tail_number VARCHAR,
    model VARCHAR,
    capacity INTEGER,
    production_year INTEGER,
    status BOOLEAN
);

CREATE TABLE IF NOT EXISTS flight (
    flight_id BIGINT,
    flight_number VARCHAR,
    departure VARCHAR,
    destination VARCHAR,
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    airplane_id BIGINT
);

CREATE TABLE IF NOT EXISTS reservation (
    reservation_id BIGINT,
    passenger_name VARCHAR,
    passenger_email VARCHAR,
    reservation_code VARCHAR,
    flight_id BIGINT,
    status BOOLEAN,
    created_at TIMESTAMP
);

-- =====================================================
-- Data: Airplane
-- =====================================================
INSERT INTO airplane (airplane_id, tail_number, model, capacity, production_year, status) VALUES
    (1, 'TC-AAB', 'Airbus A320', 180, 2015, true),
    (2, 'TC-BBC', 'Boeing 737-800', 189, 2012, true),
    (3, 'TC-CCD', 'Airbus A321', 220, 2018, true),
    (4, 'TC-DDE', 'Boeing 777-300ER', 396, 2010, false),
    (5, 'TC-EEF', 'Embraer E190', 100, 2020, true);

-- =====================================================
-- Data: Flight (original + extra)
-- =====================================================
INSERT INTO flight (flight_id, flight_number, departure, destination, departure_time, arrival_time, airplane_id) VALUES
    (1, 'TK100', 'IST', 'LHR',
     TIMESTAMP '2026-01-10 08:00:00',
     TIMESTAMP '2026-01-10 12:00:00',
     1),
    (2, 'TK200', 'IST', 'CDG',
     TIMESTAMP '2026-01-11 09:00:00',
     TIMESTAMP '2026-01-11 11:30:00',
     2),
    (3, 'PC300', 'SAW', 'AMS',
     TIMESTAMP '2026-01-12 14:00:00',
     TIMESTAMP '2026-01-12 17:30:00',
     3),
    (4, 'TK400', 'IST', 'DXB',
     TIMESTAMP '2026-01-13 22:00:00',
     TIMESTAMP '2026-01-14 04:30:00',
     4),
    (5, 'PC500', 'ESB', 'IST',
     TIMESTAMP '2026-01-14 06:30:00',
     TIMESTAMP '2026-01-14 07:45:00',
     5),

    (6, 'TK600', 'IST', 'FRA',
     TIMESTAMP '2026-01-15 08:15:00',
     TIMESTAMP '2026-01-15 11:00:00',
     1),
    (7, 'TK601', 'FRA', 'IST',
     TIMESTAMP '2026-01-16 13:30:00',
     TIMESTAMP '2026-01-16 18:10:00',
     1),
    (8, 'PC700', 'SAW', 'AYT',
     TIMESTAMP '2026-01-15 07:00:00',
     TIMESTAMP '2026-01-15 08:20:00',
     3),
    (9, 'PC701', 'AYT', 'SAW',
     TIMESTAMP '2026-01-16 20:00:00',
     TIMESTAMP '2026-01-16 21:20:00',
     3),
    (10, 'TK800', 'IST', 'JFK',
     TIMESTAMP '2026-01-17 10:00:00',
     TIMESTAMP '2026-01-17 20:30:00',
     4),
    (11, 'TK801', 'JFK', 'IST',
     TIMESTAMP '2026-01-19 22:00:00',
     TIMESTAMP '2026-01-20 13:10:00',
     4),
    (12, 'PC900', 'ESB', 'ADB',
     TIMESTAMP '2026-01-18 06:45:00',
     TIMESTAMP '2026-01-18 08:05:00',
     5);

-- =====================================================
-- Data: Reservation (original + extra)
-- =====================================================
INSERT INTO reservation (reservation_id, passenger_name, passenger_email, reservation_code, flight_id, status, created_at) VALUES
    (1, 'Mustafa Yilmaz', 'mustafa.yilmaz@example.com', 'A1B2C', 1, true,
     TIMESTAMP '2026-01-05 10:12:00'),
    (2, 'Ayse Kaya', 'ayse.kaya@example.com', 'D3E4F', 1, true,
     TIMESTAMP '2026-01-05 11:00:00'),
    (3, 'Mehmet Demir', 'mehmet.demir@example.com', 'G5H6J', 2, true,
     TIMESTAMP '2026-01-06 09:20:00'),
    (4, 'Elif Sahin', 'elif.sahin@example.com', 'K7L8M', 3, false,
     TIMESTAMP '2026-01-07 14:45:00'),
    (5, 'John Smith', 'john.smith@example.com', 'N9P0Q', 4, true,
     TIMESTAMP '2026-01-08 18:10:00'),
    (6, 'Emma Brown', 'emma.brown@example.com', 'R2S3T', 5, true,
     TIMESTAMP '2026-01-09 07:55:00'),

    (7, 'Ahmet Yilmaz', 'ahmet.yilmaz@example.com', 'U1V2W', 6, true,
     TIMESTAMP '2026-01-10 09:10:00'),
    (8, 'Zeynep Kaya', 'zeynep.kaya@example.com', 'X3Y4Z', 6, true,
     TIMESTAMP '2026-01-10 10:25:00'),
    (9, 'Ali Demir', 'ali.demir@example.com', 'A7B8C', 7, true,
     TIMESTAMP '2026-01-11 16:40:00'),
    (10, 'Fatma Sahin', 'fatma.sahin@example.com', 'D9E1F', 7, false,
     TIMESTAMP '2026-01-12 08:05:00'),

    (11, 'Mehmet Can', 'mehmet.can@example.com', 'G2H3J', 8, true,
     TIMESTAMP '2026-01-13 11:55:00'),
    (12, 'Elif Aydin', 'elif.aydin@example.com', 'K4L5M', 8, true,
     TIMESTAMP '2026-01-13 12:10:00'),

    (13, 'John Doe', 'john.doe@example.com', 'N6P7Q', 9, true,
     TIMESTAMP '2026-01-14 09:00:00'),
    (14, 'Emma Wilson', 'emma.wilson@example.com', 'R8S9T', 9, true,
     TIMESTAMP '2026-01-14 09:20:00'),

    (15, 'Michael Brown', 'michael.brown@example.com', 'U0V1W', 10, true,
     TIMESTAMP '2026-01-15 15:45:00'),
    (16, 'Olivia Taylor', 'olivia.taylor@example.com', 'X2Y3Z', 10, true,
     TIMESTAMP '2026-01-15 16:10:00'),

    (17, 'Daniel Smith', 'daniel.smith@example.com', 'A4B5C', 11, true,
     TIMESTAMP '2026-01-16 18:30:00'),
    (18, 'Sophia Johnson', 'sophia.johnson@example.com', 'D6E7F', 11, true,
     TIMESTAMP '2026-01-16 19:00:00'),

    (19, 'Kerem Ozkan', 'kerem.ozkan@example.com', 'G8H9J', 12, true,
     TIMESTAMP '2026-01-17 07:10:00'),
    (20, 'Merve Aslan', 'merve.aslan@example.com', 'K0L1M', 12, false,
     TIMESTAMP '2026-01-17 07:30:00');
