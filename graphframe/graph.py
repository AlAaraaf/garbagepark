# 创建无向图
g = GraphFrame(node, edge)
g.inDegree.describe().show()
g.edges.count()

# 查找所有连通分量
sc.setCheckpointDir('checkpointsave')
# cc:id,name,component
cc = g.connectedComponents()
cc.groupBy('component').count().show()
# ccgroup:component, count
ccgroup = cc.groupBy('component').count()
ccgroup.count()
ccgroup.sort(desc('count')).show()
ccgroup.withColumn('nums',ccgroup['count']).drop('count').groupBy('nums').count().show()
ccgroup.withColumn('nums',ccgroup['count']).drop('count').groupBy('nums').count().sort(desc('nums')).show()

# 查看最大连通分量（node nums = 780759）
maxcc = cc.filter('component == 0')
