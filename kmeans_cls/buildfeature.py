# 生成dpdate列
from pyspark.sql.functions import concat_ws, to_date, datediff
from pyspark.sql.functions import lit
data = data.withColumn('year', lit('2019'))
data = data.withColumn('dpdate', concat_ws('-', data['year'], data['month'], data['day']))
data = data.withColumn('dpdate', to_date(data['dpdate']))
data.createOrReplaceTempView('datas')

# 出行时间?（这个不是聚合值啊）与出行频率
# freq有0 -> max=min(count = 1)
# custrec:id, freq, maxd, mind
cust = spark.read.csv('passenger.csv', header = True, inferSchema = True)
cust.createOrReplaceTempView('cust')
cust_rec = spark.sql('select c.id as id, d.dpdate as time from cust c, datas d where c.id = d.id group by c.id, d.dpdate')
cust_rec = cust_rec.withColumn('head', lit('2019-01-01'))
cust_rec = cust_rec.withColumn('recency', datediff(cust_rec['time'], cust_rec['head']))
cust_rec.createOrReplaceTempView('custrec')
cust_info = spark.sql('select id, count(time) as counts, max(recency) as maxd, min(recency) as mind from custrec group by id')
cust_info = cust_info.withColumn('freq', round(cust_info['counts']/(cust_info['maxd']-cust_info['mind']),5))
cust_info = cust_info.fillna(0,'freq')
cust_info.createOrReplaceTempView('custrec')

# 周末出行占比
# custweek:id, percent
cust_week = spark.sql('select c.id as id, round(sum(case when d.weekday = 6 then 1 when d.weekday = 0 then 1 else 0 end)/count(d.weekday),2) as percent from cust c, datas d where c.id = d.id group by c.id')
cust_week.createOrReplaceTempView('custweek')

# 出行月份个数
# custmonth:id, monthnum
cust_month = spark.sql('select c.id as id, count(s.month) as monthnum from (select distinct month, id from datas) as s, cust c where c.id = s.id group by c.id')
cust_month.createOrReplaceTempView('custmonth')

cust_clust = spark.sql('select c.*, rec.freq as freq, rec.maxd as maxd, rec.mind as mind, week.percent as wpercent, month.monthnum as mnum from cust c, custrec rec, custweek week, custmonth month where c.id = rec.id and c.id = week.id and c.id = month.id')
