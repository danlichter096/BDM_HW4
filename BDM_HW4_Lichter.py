import csv
import datetime
import json
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
#from pyspark.sql.functions import collect_list
from pyspark.sql.types import DateType, IntegerType, MapType, StringType


def expandVisits(date_range_start, visits_by_day):
    '''
    This function needs to return a Python's dict{datetime:int} where:
      key   : datetime type, e.g. datetime(2020,3,17), etc.
      value : the number of visits for that day
    '''
    visits = {}
    visits_by_day = list(map(lambda x: int(x), visits_by_day.replace('[','').replace(']', '').split(',')))
    truncated_date = date_range_start[0:10]
    datetime_date = datetime.datetime.strptime(truncated_date, '%Y-%m-%d')
    for x in visits_by_day:
        visits[datetime_date] = x
        datetime_date+=datetime.timedelta(days=1)
    return visits

def main(sc):
  spark = SparkSession(sc)
  index = 0
  fileNames = ['big_box_grocers','convenience_stores','drinking_places','full_service_restaurants','limited_service_restaurants',
                'pharmacies_and_drug_stores','snack_and_bakeries','specialty_food_stores','supermarkets_except_convenience_stores']
  
  weeklydf = spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/', header = True)\
                .select('safegraph_place_id', 'date_range_start', 'visits_by_day')
  udfExpand = F.udf(expandVisits, MapType(DateType(), IntegerType()))
  datedf = weeklydf.select('safegraph_place_id',
                 F.explode(udfExpand('date_range_start', 'visits_by_day')).alias('date', 'visits'))\
                .filter(F.col("date") >= datetime.date(2019,1,1))
  NAICS = set(['452210', '452311', '445120', '722410', '722511', '722513', '446110', '446191','311811', '722515', 
             '445210','445220','445230','445291','445292','445299','445110'])
  coredf = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header = True, escape = '"')\
          .select('safegraph_place_id','naics_code')\
          .where(F.col('naics_code').isin(NAICS)) 
  joindf = coredf.join(datedf, 'safegraph_place_id')\
               .withColumn('year', F.year(F.col('date')))\
              .withColumn('date', F.when(F.col('date')< datetime.date(2020,1,1), F.add_months(F.col('date'), 12)).otherwise(F.col('date')))\
              .select('naics_code', 'year','date', 'visits')
  
  big_box_df = joindf.where(F.col('naics_code').isin([452210,452311])).drop('naics_code')
  convenience_df = joindf.where(F.col('naics_code').isin([445120])).drop('naics_code')
  drinking_df = joindf.where(F.col('naics_code').isin([722410])).drop('naics_code')
  full_service_df = joindf.where(F.col('naics_code').isin([722511])).drop('naics_code')
  limited_service_df = joindf.where(F.col('naics_code').isin([722513])).drop('naics_code')
  pharmacies_drug_df = joindf.where(F.col('naics_code').isin([446110,446191])).drop('naics_code')
  snack_bakeries_df = joindf.where(F.col('naics_code').isin([311811,722515])).drop('naics_code')
  specialty_df = joindf.where(F.col('naics_code').isin([445210,445220,445230,445291,445292,445299])).drop('naics_code')
  supermarkets_df = joindf.where(F.col('naics_code').isin([445110])).drop('naics_code')
  
  a = big_box_df.groupby('year','date').agg(F.sort_array(F.collect_list('visits')).alias('array_visits'))\
                      .withColumn('middle', F.ceil(F.size(F.col('array_visits'))/2).cast('int') ).drop('array_visits') #F.ceil((F.size(F.col('array_visits'))/2)).cast('int'))).drop('array_visits')
                      #.withColumn('median', F.element_at(F.col('array_visits'), F.col('middle')))\
                       # .drop('array_visits') #F.element_at(F.col('array_visits'), F.ceil((F.size(F.col('array_visits'))/2)).cast('int')))
  a.write.option("header",True).csv(f"{sys.argv[1]}/a")
  big_box_df.write.option("header", True).csv(f"{sys.argv[1]}/")
  #convenience_df.write.option("header",True).csv(f"{sys.argv[1]}/convenience_stores")
  #drinking_df.write.option("header",True).csv(f"{sys.argv[1]}/drinking_places")
  #full_service_df.write.option("header",True).csv(f"{sys.argv[1]}/full_service_restaurants")
  #limited_service_df.write.option("header",True).csv(f"{sys.argv[1]}/limited_service_restaurants")
  #pharmacies_drug_df.write.option("header",True).csv(f"{sys.argv[1]}/pharmacies_and_drug_stores")
  #snack_bakeries_df.write.option("header",True).csv(f"{sys.argv[1]}/snack_and_bakeries")
  #specialty_df.write.option("header",True).csv(f"{sys.argv[1]}/specialty_food_stores")
  #supermarkets_df.write.option("header",True).csv(f"{sys.argv[1]}/supermarkets_except_convenience_stores")
  #drinking_df.write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[index]}")
  #limited_service_df.write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[index]}")
  
#a_df=big_box_df.groupBy('year','date').agg(F.stddev_pop('visits').alias('std'), F.percentile_approx('visits', 0.5).alias('median'))
  #a_df.write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[index]}")
  #dfs = [big_box_df, convenience_df, drinking_df, full_service_df, limited_service_df, pharmacies_drug_df, snack_bakeries_df, specialty_df, supermarkets_df]
  #big_box_df.write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[index]}")
    #for x in range(len(dfs)):
  #  dfs[x].write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[x]}")
#a_df = big_box_df.groupBy('year','date').agg(F.stddev_pop('visits').alias('std'), F.sort_array(F.collect_list('visits')).alias('array1'))\
  #      .withColumn('median', F.element_at(F.col('array1'), F.ceil((F.size(F.col('array1'))/2)).cast('int')))\
  #      .withColumn('std', F.round('std').cast('int'))\
  #      .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
  #      .withColumn('high', F.col('median')+F.col('std'))\
  #      .drop('array1')\
  #      .drop('std')
  
  
  # dfs[x].write.option("header",True).csv(f"{sys.argv[1]}/{fileNames[index]}")
  #  index+=1


if __name__ == "__main__":
  sc = pyspark.SparkContext()
  #print(sc.version)
  main(sc)

    
                

  
