import pyspark as ps
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
import pandas as pd

sc = SparkContext("local", "garbage")
spark = SparkSession(sc)

data  = spark.read.csv('data0128-01.csv', header = True, inferSchema = True)
maxcc = spark.read.csv('maxcc.csv', header = True, inferSchema = True)
data.createOrReplaceTempView('datas')
maxcc.createOrReplaceTempView('max')
cust_mc = spark.sql('select d.* from datas d, max m where d.id = m.id')
cust_mc = cust_mc.drop('code','tail','year')
#cust_mc.coalesce(1).write.format('csv').option('header','true').save('custmc')

# 连通路径
path = spark.sql("select dpstation, rcstation, count(*) as num from datas group by dpstation, rcstation order by num desc")

#顾客信息
data = spark.read.csv('custmc.csv', header = True, inferSchema = True)
cust = spark.read.csv('passenger.csv', header = True, inferSchema = True)
mccust = spark.sql('select distinct c.* from cust c, datas d where d.id = c.id')
