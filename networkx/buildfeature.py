data = spark.read.format('com.databricks.spark.csv').option('header','true').load('data0201.csv')
from pyspark.sql.functions import udf, StringType
def zerofill(x):
    return x.zfill(2)

strzfill = udf(zerofill, StringType())

data_0 = data.withColumn('month',strzfill(data['month']))\
.withColumn('day',strzfill(data['day']))\
.withColumn('hour',strzfill(data['hour']))\
.withColumn('year',lit('2019'))

# 数据合并
from pyspark.sql.functions import to_timestamp
data_1 = data_0.withColumn('dptime0', concat_ws('-', data_0['year'], data_0['month'], data_0['day']))
data_2 = data_1.withColumn('dptime1', concat_ws(' ', data_1['dptime0'], data_1['hour']))
data_2 = data_2.withColumn('tail',lit('00:00'))
data_3 = data_2.withColumn('dptime2', concat_ws(':',data_2['hour'],data_2['tail']))
data_use = data_3.withColumn('dptime',to_timestamp(data_3['dptime2']))\
.drop('dptime0','dptime1', 'dptime2')

# 关联旅客
data_use.createOrReplaceTempView('datas')
path_distc = spark.sql('select dpstation, rcstation, dptime, count(*) as num from datas group by dpstation, rcstation, dptime order by num desc')

#设置index
from pyspark.ml.feature import StringIndexer
path_distc = path_distc.withColumn('id',concat(path_distc['dpstation'], path_distc['rcstation'], path_distc['dptime']))
indexer = StringIndexer(inputCol = 'id',outputCol = 'idcode', stringOrderType = 'frequencyAsc')
pathcode = indexer.fit(path_distc)
path_withcode = pathcode.transform(path_distc)
path_withcode.createOrReplaceTempView('paths')

data_withcode = spark.sql('select p.idcode as code, d.* from paths p, datas d where p.dpstation = d.dpstation and p.rcstation = d.rcstation and p.dptime = d.dptime')
data_withcode.coalesce(1).write.format('csv').option('header','true').save('middata')


together = spark.sql('select d1.id as id1, d2.id as id2, count(*) as num from datac d1, datac d2 where d1.code = d2.code and not d1.id = d2.id group by d1.id, d2.id having num > 1 order by num desc')
