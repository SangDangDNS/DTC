## Week 1
---
### Homework Part 1 (Docker & SQL)
---
#### Question 1. Knowing docker tags
Run the command to get information on Docker
`docker --help`
Now run the command to get help on the "docker build" command
Which tag has the following text? - Write the image ID to the file
```
$ docker build --help | grep "Write the image ID to the file"
--iidfile string        Write the image ID to the file
```
**Answer:** --iidfile string
#### Question 2. Understanding docker first run
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?
```c
$ sudo docker run -it python:3.9 pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.40.0

[notice] A new release of pip is available: 23.0.1 -> 23.1.2
[notice] To update, run: pip install --upgrade pip
```
**Answer:** 3 
#### Prepare Postgres
Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

`wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz`

You will also need the dataset with zones:

`wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

#### Question 3. Count records
How many taxi trips were totally made on January 15?
```
SELECT COUNT(*)
FROM green_taxi
WHERE lpep_pickup_datetime::date = '2019-01-15'
    AND lpep_dropoff_datetime::date = '2019-01-15'
```
**Answer:** 20530
#### Question 4. Largest trip for each day
Which was the day with the largest trip distance Use the pick up time for your calculations.
```
SELECT lpep_pickup_datetime::date
FROM green_taxi
GROUP BY lpep_pickup_datetime::date
ORDER BY MAX(trip_distance) DESC
LIMIT 1
```
**Answer:** 2019-01-15
#### Question 5. The number of passengers
In 2019-01-01 how many trips had 2 and 3 passengers?
```
SELECT passenger_count, COUNT(*)
FROM green_taxi
WHERE lpep_pickup_datetime::date = '2019-01-01'
	AND (passenger_count = 2 OR passenger_count = 3)
GROUP BY passenger_count
```
**Answer:** 2: 1282 ; 3: 254
#### Question 6. Largest tip
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.  
**Note:** it's not a typo, it's tip , not trip
```
SELECT "Zone"
FROM taxi_zone_lookup
WHERE "LocationID" = (
	SELECT "DOLocationID"
	FROM green_taxi
	WHERE "PULocationID" = (
		SELECT "LocationID"
		FROM taxi_zone_lookup
		WHERE "Zone" = 'Astoria'
	)
	GROUP BY "DOLocationID"
	ORDER BY MAX(tip_amount) DESC
	LIMIT 1
)
```
**Answer:** Long Island City/Queens Plaza
