# 查看缺失值
from pyspark.sql.functions import isnull
for i in data.columns:
    num = data.filter(isnull(i)).count()
    print(num,": ",i)
    
# 查看异常值
data.describe("age").show()
data.filter("age < 1 or age > 100").count()
data_use = data.filter("age >= 1 and age <= 100")

# 开头如果重新启动集群注意重新启动spark
# 用这一句读可以减小数据类型转换的工作量
data = spark.read.format('com.databricks.spark.csv')\
.option('header','true')\
.load('data0127-01.csv')

# 修改变量类型
data_t = data.withColumn('age',data['age'].cast('int'))\
.withColumn('dpcount',data['dpcount'].cast('int'))\
.withColumn('rccount',data['rccount'].cast('int'))\
.withColumn('dplat',data['dplat'].cast('double'))\
.withColumn('dplon',data['dplon'].cast('double'))\
.withColumn('rclat',data['rclat'].cast('double'))\
.withColumn('rclon',data['rclon'].cast('double'))\
.drop('province','dptime','dpdate')

# 描述性统计
spark.sql('select count(distinct(id)) as num from datas ').show()
# 总共有2010238人
# 离谱的是一个人的记录下年龄和性别可能不一样
# 所以dinstinct id, age, sex的结果不一样
# 现在取的是每个人下的第一条年龄和性别记录
spark.sql("select distinct(id) as id,age,sex from datas").filter('id == 91314261').show()

# 分组统计与画图数据提取
from pyspark.sql import functions as fun
data.groupBy('sex','dpprovince').count().show()
data.groupBy('sex','rcprovince').count().show()
# 出发省只有云南省

data.describe('age').show()
data_group_month = data.groupBy('month').count()
data_group_weekday = data.groupBy('weekday').count()

data.createOrReplaceTempView('datas')
path = spark.sql("select dpstation, rcstation, count(*) as num from datas group by dpstation, rcstation order by num desc")

# 分组统计数据应该使用passenger表进行
# agegroup
from pyspark.ml.feature import Bucketizer
splits = [0,10,20,30,40,50,60,70,80,90,100]
bucketizer = Bucketizer(splits = splits, inputCol = 'age',outputCol = 'agegroup')
cust_group_age = bucketizer.setHandleInvalid('keep').transform(cust)
cust_group_age = cust_group_age.groupBy('agegroup').count()
cust_group_age.coalesce(1).write.format('csv').option('header', 'true').save('custage')

# sex
cust_group_sex = cust.groupBy('sex').count()
cust_group_sex.coalesce(1).write.format('csv').option('header', 'true').save('custsex')

# freq
data = data.withColumn('year', lit('2020'))
data = data.withColumn('dpdate', concat_ws('-', data['year'], data['month'], data['day']))
data = data.withColumn('dpdate', to_date(data['dpdate']))
data.createOrReplaceTempView('datas')
cust_rec = spark.sql('select c.id as id, d.dpdate as time from cust c, datas d where c.id = d.id group by c.id, d.dpdate')
cust_rec = cust_rec.withColumn('head', lit('2020-01-01'))
cust_rec = cust_rec.withColumn('recency', datediff(cust_rec['time'], cust_rec['head']))
cust_rec.createOrReplaceTempView('custrec')
cust_info = spark.sql('select id, count(time) as counts, max(recency) as maxd, min(recency) as mind from custrec group by id')
cust_info = cust_info.withColumn('freq', round(cust_info['counts']/(cust_info['maxd']-cust_info['mind']),5))
cust_info = cust_info.fillna(0,'freq')
cust_info.coalesce(1).write.format('csv').option('header', 'true').save('custfreq')

# station_num
dpstation_num = data.groupBy('dpstation').count()
rcstation_num = data.groupBy('rcstation').count()
dpnum = dpstation_num.withColumnRenamed('dpstation','station').withColumnRenamed('count','dpnum')
rcnum = rcstation_num.withColumnRenamed('rcstation','station').withColumnRenamed('count','rcnum')
station_num = dpnum.join(rcnum, on = ['station'], how = 'outer')
station_num = station_num.withColumn('num',station_num['dpnum']+station_num['rcnum'])
max_station = station_num.sort(desc('num')).filter('station like "%站"')
max_station.coalesce(1).write.format('csv').option('header', 'true').save('maxstation')
