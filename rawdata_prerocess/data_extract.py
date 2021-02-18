import pyspark as ps
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
import pandas as pd

sc = SparkContext("local", "garbage")
spark = SparkSession(sc)

data = spark.read.csv("data.csv", header=True, inferSchema=True)
#data.count() #总数据量：58471093
data.createOrReplaceTempView('rowdata')

path = spark.sql("select dpstation, rcstation, count(*) as num from rowdata group by dpstation, rcstation having num > 2000 order by num desc")
#path.count() # 符合条件的总条数为
path.createOrReplaceTempView('paths')

dplist = spark.sql("select dpstation, count(*) as dpsum, sum(num) as dpcount from paths group by dpstation")
rclist = spark.sql("select rcstation, count(*) as rcsum, sum(num) as rccount from paths group by rcstation")

dplist.createOrReplaceTempView('dplists')
rclist.createOrReplaceTempView('rclists')

# new edition
stationlist = spark.sql("select * from dplists full outer join rclists on dplists.dpstation = rclists.rcstation")
stationlist.createOrReplaceTempView('stationlists')
#stationlist.count() 

stationlist_c = spark.sql("select dpstation as station, dpsum, rcsum, dpcount, rccount from stationlists")

from pyspark.sql.functions import col
stationlist_c = stationlist_c.na.fill(0)
stationlist_c = stationlist_c.withColumn("pathnum",col("dpsum")+col("rcsum"))
stationlist_c = stationlist_c.withColumn("pathcount",col("dpcount")+col("rccount"))

data_use1 = spark.sql("select r.*, st1.province as dpprovince, st1.count as dpcount, st1.lat as dplat, st1.lon as dplon, st2.province as rcprovince, st2.count as rccount, st2.lat as rclat, st2.lon as rclon from rawdata r, stationlist st1, stationlist st2 where st1.province = '云南省' and r.dpstation =st1.station and r.rcstation = st2.station")

data_use2 = spark.sql(" select r.*, st1.province as rcprovince, st1.count as rccount, st1.lat as rclat, st1.lon as rclon, st2.province as dpprovince, st2.count as dpcount, st2.lat as dplat, st2.lon as dplon from rawdata r, stationlist st1, stationlist st2 where st1.province = '云南省' and r.rcstation = st1.station and r.dpstation = st2.station")

data_use = data_use1.union(data_use2)

cols = ("province","dpdate","dptime")
data_use.drop(*cols).printSchema()
data_use = data_use.distinct()
