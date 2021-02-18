from pyspark.ml.clustering import KMeans
# 查找最佳k值
for i in range(2,11):
    km = KMeans().setK(i).setSeed(4603).setFeaturesCol('feature').setPredictionCol('prediction')
    res_kmval = km.fit(clsdata_model).summary.trainingCost
    print(i,': ',res_kmval)
    

# k = 4
model = KMeans().setK(3).setSeed(4603).setFeaturesCol('feature').setPredictionCol('prediction').fit(clsdata_model)
res_km = model.transform(clsdata_model)
summary = model.summary
summary.clusterSizes
[739011, 463649, 807578]
summary.trainingCost
>>> 7632810.723481619
model.clusterCenters()

model.save('kmeans3_model')
clsdata_vecform.createOrReplaceTempView('clsdata')
res_km.createOrReplaceTempView('reskm')
res4 = spark.sql('select c.*, r.prediction as prediction from clsdata c, reskm r where c.id = r.id').drop('feature')
