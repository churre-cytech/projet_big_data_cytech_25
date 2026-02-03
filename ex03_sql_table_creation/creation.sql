-- DROP TABLE
DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_store_and_fwd;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_ratecode;
DROP TABLE IF EXISTS dim_vendor;

-- CREATE TABLE
CREATE TABLE dim_vendor (
  vendor_id SMALLINT PRIMARY KEY,
  vendor_name TEXT
);

CREATE TABLE dim_ratecode (
  ratecode_id SMALLINT PRIMARY KEY,
  ratecode_name TEXT
);

CREATE TABLE dim_payment (
  payment_type_id SMALLINT PRIMARY KEY,
  payment_type_name TEXT
);

CREATE TABLE dim_location (
  location_id INT PRIMARY KEY,
  borough TEXT,
  zone TEXT,
  service_zone TEXT
);

CREATE TABLE dim_date (
  date_id INT PRIMARY KEY, -- format yyyymmdd, ex: 20250501
  date_value DATE NOT NULL,
  year SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day SMALLINT NOT NULL,
  day_of_week SMALLINT NOT NULL
);

CREATE TABLE dim_store_and_fwd (
  flag CHAR(1) PRIMARY KEY,
  description TEXT
);

CREATE TABLE fact_trip (
  trip_id BIGSERIAL PRIMARY KEY,

  vendor_id SMALLINT REFERENCES dim_vendor(vendor_id),
  ratecode_id SMALLINT REFERENCES dim_ratecode(ratecode_id),
  payment_type_id SMALLINT REFERENCES dim_payment(payment_type_id),
  store_and_fwd_flag CHAR(1) REFERENCES dim_store_and_fwd(flag),

  pickup_location_id INT REFERENCES dim_location(location_id),
  dropoff_location_id INT REFERENCES dim_location(location_id),

  pickup_date_id INT REFERENCES dim_date(date_id),
  dropoff_date_id INT REFERENCES dim_date(date_id),

  pickup_datetime TIMESTAMP NOT NULL,
  dropoff_datetime TIMESTAMP NOT NULL,

  passenger_count BIGINT,
  trip_distance DOUBLE PRECISION,
  fare_amount DOUBLE PRECISION,
  extra DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION,
  airport_fee DOUBLE PRECISION,
  cbd_congestion_fee DOUBLE PRECISION,
  total_amount DOUBLE PRECISION
);
