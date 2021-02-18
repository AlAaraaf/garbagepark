import networkx as net
g = net.Graph()
# 存储成csv再使用pandas读入
node1 = list(edge['id1'])
node2 = list(edge['id2'])
value = list(edge['num'])

nodelist = list(set(node1))
g.add_nodes_from(nodelist)

for i in range(0, len(value)):
    g.add_weighted_edges_from([(node1[i], node2[i], value[i])])
    if i%10000 == 0:
        print(i," ", end = "")

net.info(g)
net.density(g)
degree = pd.DataFrame(net.degree_histogram(g))
degree[0].describe()

# 求子网数量与大小
cc = net.connected_components(g)
count = 0
num_of_node = []
num_of_edge = []
avr_degree = []

for c in cc:
    count = count + 1
    subg = g.subgraph(c)
    num_of_node.append(subg.number_of_nodes())
    num_of_edge.append(subg.number_of_edges())
    patt = re.compile(r'[0-9]+\.[0-9]+')
    avr_degree.append(patt.findall(net.info(subg))[0])
