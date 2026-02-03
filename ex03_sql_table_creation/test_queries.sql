-- Total revenue by vendor
SELECT v.vendor_name, SUM(f.total_amount) AS total_revenue
FROM fact_trip f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY total_revenue DESC;

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



