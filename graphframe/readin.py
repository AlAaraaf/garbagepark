from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from graphframes import *

edge = spark.read.csv('edge.csv', header = True, inferSchema = True)
edge = edge.withColumnRenamed('id1','src').withColumnRenamed('id2','dst')
edge = edge.filter('num > 2')

# 创建结点表的id列
# 格式：id,name
node = edge.select('src')
node = node.withColumn('id', node['src'])
node = node.withColumn('name', node['id']).drop('src')
node = node.distinct()

# 之后读取数据
edge = spark.read.csv('netedge.csv', header = True, inferSchema = True)
node = spark.read.csv('netnode.csv', header = True, inferSchema = True)
