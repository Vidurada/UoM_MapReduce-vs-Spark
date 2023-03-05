/* load data from s3 as a dataframe */
val df = spark.read.format("csv").option("header", "true").option("interschema", "true").load("s3a://cs5229-239307b-vidura/emr-assignment/DelayedFlights-updated.csv")

/* create a temporary view */
df.createOrReplaceTempView("flight_delays")


/* year wise carrier delay from 2003-2010 */
val sqlDF = spark.sql("SELECT Year, COUNT(*) as CarrierDelays FROM flight_delays where CarrierDelay > 0 GROUP BY Year")
spark.time(sqlDF.show())

/* year wise nas delay from 2003-2010 */
val sqlDF = spark.sql("SELECT Year, COUNT(*) as CarrierDelays FROM flight_delays where NASDelay > 0 GROUP BY Year")
spark.time(sqlDF.show())

/* year wise weather delay from 2003-2010 */
val sqlDF = spark.sql("SELECT Year, COUNT(*) as CarrierDelays FROM flight_delays where WeatherDelay > 0 GROUP BY Year")
spark.time(sqlDF.show())

/* year wise late aircraft delay from 2003-2010 */
val sqlDF = spark.sql("SELECT Year, COUNT(*) as CarrierDelays FROM flight_delays where LateAircraftDelay > 0 GROUP BY Year")
spark.time(sqlDF.show())

/* year wise security delay from 2003-2010 */
val sqlDF = spark.sql("SELECT Year, COUNT(*) as CarrierDelays FROM flight_delays where SecurityDelay > 0 GROUP BY Year")
spark.time(sqlDF.show())


