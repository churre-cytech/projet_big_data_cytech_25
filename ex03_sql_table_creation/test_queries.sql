-- Total revenue by vendor
SELECT v.vendor_name, SUM(f.total_amount) AS total_revenue
FROM fact_trip f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY total_revenue DESC;

-- Check how many lines per vendor
SELECT vendor_id, COUNT(*) 
FROM fact_trip 
GROUP BY vendor_id;

-- Average trip distance by ratecode
SELECT r.ratecode_name, AVG(f.trip_distance) AS avg_distance
FROM fact_trip f
JOIN dim_ratecode r ON f.ratecode_id = r.ratecode_id
GROUP BY r.ratecode_name
ORDER BY avg_distance DESC;

-- Trips by payment type
SELECT p.payment_type_name, COUNT(*) AS trip_count
FROM fact_trip f
JOIN dim_payment p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name
ORDER BY trip_count DESC;

-- Average distance per trip
SELECT AVG(trip_distance) AS avg_trip_distance
FROM fact_trip;


-- ############ Queries test for Streamlit dashboard ############
-- Query for KPIs 
SELECT
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    AVG(CASE WHEN total_amount > 0 THEN tip_amount / total_amount END) AS tip_rate
FROM fact_trip;

-- Trips by day of week
SELECT d.day_of_week, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_date d ON f.pickup_date_id = d.date_id
GROUP BY d.day_of_week
ORDER BY d.day_of_week;

-- Revenue by hour
SELECT EXTRACT(HOUR FROM pickup_datetime) AS hour,
        SUM(total_amount) AS revenue
FROM fact_trip
GROUP BY hour
ORDER BY hour;

-- Average fare by payment type
SELECT p.payment_type_name, AVG(f.total_amount) AS avg_fare
FROM fact_trip f
JOIN dim_payment p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name
ORDER BY avg_fare DESC;

-- Average tip rate by vendor
SELECT v.vendor_name,
    AVG(CASE WHEN f.total_amount > 0 THEN f.tip_amount / f.total_amount END) AS tip_rate
FROM fact_trip f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY tip_rate DESC;

-- Top 10 pickup zones
SELECT l.borough, l.zone, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_id = l.location_id
GROUP BY l.borough, l.zone
ORDER BY trips DESC
LIMIT 10;