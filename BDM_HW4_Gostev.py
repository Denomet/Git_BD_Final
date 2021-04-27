from pyspark import SparkContext
import sys
import csv
import json
import datetime
from operator import add
import statistics
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

if __name__=='__main__':
    sc = SparkContext()

    NAICS_dict = {
		"452210": "big_box_grocers",
		"452311": "big_box_grocers",
		"445120": "convenience_stores",
		"722410": "drinking_places",
		"722511": "full_service_restaurants",
		"722513": "limited_service_restaurants",
		"446110": "pharmacies_and_drug_stores",   
		"446191": "pharmacies_and_drug_stores",
		"311811": "snack_and_bakeries",
		"722515": "snack_and_bakeries",
		"445210": "specialty_food_stores",
		"445220": "specialty_food_stores",
		"445230": "specialty_food_stores",
		"445291": "specialty_food_stores",
		"445292": "specialty_food_stores",
		"445299": "specialty_food_stores",
		"445110": "supermarkets_except_convenience_stores"}

	def addCategory(x):
  		return(x[0], NAICS_dict[x[1]])

	places = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv') \
	  	.map(lambda x: x.split(',')) \
		.map(lambda x: (x[1], x[9])) \
		.filter(lambda x:  x[1] in NAICS_dict.keys()) \
		.map(lambda x: addCategory(x)) \
		.collect()

	places_dict = {}
	places_dict = dict(places)

	def convertKey(x):
  		for i in range(7):
		    date =(datetime.datetime.strptime(x[1][:10], '%Y-%m-%d').date() + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
		    visits=json.loads(x[2])[i]
		    yield ((date, places_dict[x[0]]), [x[0], visits])

	def MedLowHigh(x):
		if len(x) > 0:
	    	median_vst = statistics.median([item[1] for item in x[1]])

	    if len(x[1]) == 1:
	     	sd_vst = 0
	    else:
	     	sd_vst = statistics.stdev([item[1] for item in x[1]])

	    high_vst = median_vst + sd_vst
	    low_vst = median_vst - sd_vst
	    if low_vst < 0:
	     	low_vst = 0

	    real_year = datetime.datetime.strptime(x[0][0], '%Y-%m-%d').year
	    project_date =  '2020-' + x[0][0][5:] 
	    place_type = x[0][1] 
	    return (real_year, project_date, int(median_vst), int(low_vst), int(high_vst), place_type)

		patterns = sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020.csv') \
			.map(lambda x: next(csv.reader([x]))) \
			.filter(lambda x: x[1] in places_dict.keys()) \
			.map(lambda x: (x[1], x[12], x[16] )) \
			.flatMap(lambda x: convertKey(x)) \
			.groupByKey() \
			.map(lambda x: (x[0], list(x[1]) )) \
			.map(lambda x: MedLowHigh(x)) \
			.filter(lambda x: x[0] > 2018) \
			.sortBy(lambda x: (x[0], x[1])) \
			.collect() 
		schema = StructType([ \
		    StructField("year",StringType(),True), \
		    StructField("date",StringType(),True), \
		    StructField("median",IntegerType(),True), \
		    StructField("low", IntegerType(), True), \
		    StructField("high", IntegerType(), True), \
		    StructField("type", StringType(), True)])

		spark = SparkSession.builder.getOrCreate()
		df = spark.createDataFrame(data=patterns,schema=schema)

		df.write.option("header",True) \
	        .partitionBy("type") \
	        .mode("overwrite") \
	        .csv("/test")