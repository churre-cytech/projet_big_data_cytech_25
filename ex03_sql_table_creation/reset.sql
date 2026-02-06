TRUNCATE TABLE
    fact_trip,
    dim_date,
    dim_location,
    dim_store_and_fwd,
    dim_payment,
    dim_ratecode,
    dim_vendor
RESTART IDENTITY CASCADE;
