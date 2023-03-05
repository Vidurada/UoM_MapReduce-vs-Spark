/* create storage integration */
CREATE STORAGE INTEGRATION S3_Snowflake 
	TYPE = EXTERNAL_STAGE 
	STORAGE_PROVIDER = S3
	ENABLED = TRUE 
	STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::760929102675:role/snowflake-role'
	STORAGE_ALLOWED_LOCATIONS = ('s3://cs5229-239307b-vidura/')

/* create external stage */
CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = S3_Snowflake
  URL = 's3://cs5229-239307b-vidura/emr-assignment/'
  file_format = (type = 'CSV');

/* create table */
CREATE TABLE flight_delays (
Id  SMALLINT,
Year  SMALLINT,
Month  TINYINT,
DayofMonth  TINYINT,
DayOfWeek  TINYINT,
DepTime  SMALLINT,
CRSDepTime  SMALLINT,
ArrTime  SMALLINT,
CRSArrTime  SMALLINT,
UniqueCarrier  STRING,
FlightNum  SMALLINT,
TailNum  STRING,
ActualElapsedTime  SMALLINT,
CRSElapsedTime  SMALLINT,
AirTime  SMALLINT,
ArrDelay  SMALLINT,
DepDelay  SMALLINT,
Origin  STRING,
Dest  STRING,
Distance  SMALLINT,
TaxiIn  SMALLINT,
TaxiOut  SMALLINT,
Cancelled  STRING,
CancellationCode  STRING,
Diverted  TINYINT,
CarrierDelay  SMALLINT,
WeatherDelay  SMALLINT,
NASDelay  SMALLINT,
SecurityDelay  SMALLINT,
LateAircraftDelay  SMALLINT)

/* load data into flight_delays table */
COPY INTO flight_delays FROM @my_s3_stage on_error = continue

/* year wise carrier delay from 2003-2010 */
SELECT Year, COUNT(*) as CarrierDelays
FROM flight_delays
where CarrierDelay > 0
GROUP BY Year;

/* year wise nas delay from 2003-2010 */
SELECT Year, COUNT(*) as NASDelays
FROM flight_delays
where NASDelay > 0
GROUP BY Year;

/* year wise weather delay from 2003-2010 */
SELECT Year, COUNT(*) as WeatherDelays
FROM flight_delays
where WeatherDelay > 0
GROUP BY Year;

/* year wise late aircraft delay from 2003-2010 */
SELECT Year, COUNT(*) as LateAircraftDelays
FROM flight_delays
where LateAircraftDelay > 0
GROUP BY Year;

/* year wise security delay from 2003-2010 */
SELECT Year, COUNT(*) as SecurityDelays
FROM flight_delays
where SecurityDelay > 0
GROUP BY Year;

