import csv
import datetime
import json
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
              .withColumn('date', F.when(F.col('date')< datetime.date(2020,1,1), F.add_months(F.col('date'), 12)).otherwise(F.col('date')))
            #  .select('naics_code', 'year','date', 'visits')
  
  joindf.where(F.col('naics_code').isin([452210,452311]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2)-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/big_box_grocers")

  joindf.where(F.col('naics_code').isin([445120]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2)-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/convenience_stores")
  '''
  joindf.where(F.col('naics_code').isin([722410]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/drinking_places")
  
  joindf.where(F.col('naics_code').isin([722511]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/full_service_restaurants")
  
  joindf.where(F.col('naics_code').isin([722513]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/limited_service_restaurants")
  
  joindf.where(F.col('naics_code').isin([446110,446191]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/pharmacies_and_drug_stores")
  
  joindf.where(F.col('naics_code').isin([311811,722515]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/snack_and_bakeries")
  
  joindf.where(F.col('naics_code').isin([445210,445220,445230,445291,445292,445299]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/specialty_food_stores")
  
  joindf.where(F.col('naics_code').isin([445110]))\
              .groupby('year','date').agg(F.stddev_pop('visits').cast('int').alias('std'), F.sort_array(F.collect_list('visits')).alias('array_visits'))\
              .withColumn('median', F.col('array_visits')[F.ceil(F.size(F.col('array_visits'))/2).cast('int')-F.lit(1)])\
              .withColumn('low', F.when(F.col('median')-F.col('std')>0,F.col('median')-F.col('std')).otherwise(0))\
              .withColumn('high', F.col('median')+F.col('std')).drop('array_visits').drop('std')\
              .write.option("header", True).csv(f"{sys.argv[1]}/supermarkets_except_convenience_stores")
  '''        
 

if __name__ == "__main__":
  sc = pyspark.SparkContext()
  #print(sc.version)
  main(sc)

    
                

  
