from pyspark import SparkContext
import datetime
import csv
import functools
import json
import numpy as np
import sys
import statistics

if __name__=='__main__':
    sc = SparkContext()

    rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    OUTPUT_PREFIX = sys.argv[1]
# NAICS code lookup
#===================
    CAT_CODES = set(["452210", "452311", "445120", "722410", "722511", "722513", "446110", "446191", 
             "311811", "722515", "445210", "445220", "445230", "445291", "445292", "445299", "445110"])
    
    CAT_GROUP = {
      "452210": 0,
      "452311": 0,
      "445120": 1,
      "722410": 2,
      "722511": 3,
      "722513": 4,
      "446110": 5,   
      "446191": 5,
      "311811": 6,
      "722515": 6,
      "445210": 7,
      "445220": 7,
      "445230": 7,
      "445291": 7,
      "445292": 7,
      "445299": 7,
      "445110": 8
    } 
    
    def filterPOIs(_, lines):
      import csv
      reader = csv.reader(lines)
      for line in lines:
        elem = line.split(',')
        if elem[9] in CAT_CODES:
          yield(elem[0], CAT_GROUP[elem[9]])

    rddD = rddPlaces.mapPartitionsWithIndex(filterPOIs) \
            .cache()

    storeGroup = dict(rddD.collect())

    groupCount = rddD \
        .map(lambda x: (x[1], 1)) \
        .reduceByKey(lambda x,y: x+y) \
        .sortByKey() \
        .map(lambda x: (x[1])) \
        .collect()

    def extractVisits(storeGroup, _, lines):
      import csv
      reader = csv.reader(lines)
      for line in reader:
        if line[12] >= '2019-01-01T00:00:00-00:00' and line[12] < '2021-01-00T00:00:00-00:00':
          if line[0] in storeGroup:
            group = storeGroup[line[0]]
            for i in range(7):
              days =(datetime.datetime.strptime(line[12][:10], '%Y-%m-%d').date() 
                  - datetime.datetime.strptime('2019-01-01', '%Y-%m-%d').date()).days 
              count = json.loads(line[16])[i]
              yield ((group,days), count)

    rddG = rddPattern \
        .mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))

    def computeStats(groupCount, _, records):
        for row in records:
          vists_per_store = list(row[1])  #list of num of visits for present stores
          n_present_stores = len(vists_per_store)   #num of present stores
          n_stores = groupCount [row[0][0]]   #total number of stores in this group
          list_nostores = [0] * (n_stores-n_present_stores)   #create list of 0 visits for missing stores
          list_nostores.extend(vists_per_store)  #add lists for stores present and missing

          median_vst = statistics.median(list_nostores)
          sd_vst = statistics.stdev(list_nostores)
          high_vst = median_vst + sd_vst
          low_vst = median_vst - sd_vst
          if low_vst < 0:
            low_vst = 0
          if high_vst < 0:
            high_vst = 0

          start_date = '2019-01-01'
          delta_day = row[0][1]
          date =(datetime.datetime.strptime(start_date, '%Y-%m-%d').date() + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
          year = date[0:4] 
          date = '2020-' + date[5:] 
          one_str = year+','+date+','+str(int(median_vst))+','+str(low_vst)+','+str(high_vst)
          yield ((row[0][0]), (one_str))

#     def computeStats1(groupCount, _, records):
#         yield (1)

    rddH = rddG.groupByKey() \
		.mapPartitionsWithIndex(functools.partial(computeStats, groupCount))

    rddJ = rddH.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()
    # rddJ.take(5)

#Print output files
    # OUTPUT_PREFIX = '/content/output'
    filename = 'big_box_grocers'
    rddJ.filter(lambda x: x[0]==0 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'convenience_stores'
    rddJ.filter(lambda x: x[0]==1 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'drinking_places'
    rddJ.filter(lambda x: x[0]==2 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'full_service_restaurants'
    rddJ.filter(lambda x: x[0]==3 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)
        
    filename = 'limited_service_restaurants'
    rddJ.filter(lambda x: x[0]==4 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'pharmacies_and_drug_stores'
    rddJ.filter(lambda x: x[0]==5 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'snack_and_bakeries'
    rddJ.filter(lambda x: x[0]==6 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'specialty_food_stores'
    rddJ.filter(lambda x: x[0]==7 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)

    filename = 'supermarkets_except_convenience_stores'
    rddJ.filter(lambda x: x[0]==8 or x[0]==-1).values() \
        .saveAsTextFile(sys.argv[1]+'/'+filename)
