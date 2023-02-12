* Q1:
    ``` 
    select count(1) from fhv_data;
    ```
* Q3:
    ``` 
    select count(*) from fhv_data
    where PUlocationID is NULL and DOlocationID is NULL;
    ```
* Q5:
    ``` 
    select distinct Affiliated_base_number from fhv_data
    where DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';;
    ```



