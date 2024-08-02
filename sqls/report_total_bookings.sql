-- SQLite
SELECT
    p.country_code,
    COUNT(DISTINCT b.id) AS total_bookings
FROM
    booking_gold b
LEFT JOIN passenger_gold p
    ON b.id_passenger = p.id
WHERE
    p.date_registered >= '2021-01-01 00:00:00'
GROUP BY
    p.country_code
ORDER BY
    total_bookings DESC