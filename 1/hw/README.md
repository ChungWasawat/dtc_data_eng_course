Q3:
    select count(lpep_pickup_datetime) from green_taxi_data
    where lpep_pickup_datetime >= '2019-01-15' and lpep_pickup_datetime < '2019-01-16';
Q4:
    SELECT lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance FROM green_taxi_data
    WHERE trip_distance = ( SELECT MAX(trip_distance) FROM green_taxi_data);
Q5:
    select passenger_count, count(passenger_count) from green_taxi_data
    where lpep_pickup_datetime >= '2019-01-01' and lpep_pickup_datetime < '2019-01-02'
    group by passenger_count having passenger_count = 2 or passenger_count = 3;
Q6:
    with green_zones as 
    (select concat(zpu."Borough", '/', zpu."Zone") as "pickup_loc", concat(zdo."Borough", '/', zdo."Zone") as "dropoff_loc", 
    tip_amount, zpu."Zone" as pickup_zone
    from green_taxi_data t, zones zpu, zones zdo
    where t."PULocationID" = zpu."LocationID" and t."DOLocationID" = zdo."LocationID")

    select dropoff_loc, tip_amount
    from green_zones
    where pickup_zone = 'Astoria'
    order by tip_amount desc limit 1;

