

import pyspark
from pyspark import SparkContext
import os
import sys
from collections import defaultdict
from itertools import combinations
from math import sqrt, pow
import networkx as nx
import csv as csv
#from pyspark.graphframes import graphframe as GF
from pyspark.sql import SQLContext
from pyspark.sql import Column, DataFrame, SQLContext
from pyspark.storagelevel import StorageLevel

class Betweenness():
    def __init__(self, node_lst, edge_lst, fn1):
        self.node_lst = node_lst
        self.edge_lst = edge_lst
        self.fn1 = fn1
   
    def calc_btw(self):

        g = nx.Graph()
        g = g.to_undirected()

        #g.add_nodes_from(self.node_lst)
        g.add_edges_from(self.edge_lst)

        nnodess = g.nodes()
        betw = {}
        communities = list(nx.connected_components(g))

        for node in nnodess:
            sh_path = {}
            c = 0 
            n_score = {}
            e_score = {}
            neighbor_lst = []    
            com_lvl = []  
            parents = {}
            children = {}
            l_dct = {}
            neighbor_lst.append(node)
            community = []
            for p in communities: 
                tmp_p = list(p)
                if node in tmp_p:
                    community = tmp_p
    
            while len(neighbor_lst) > 0:
                dff = []
                l_dct[c] = neighbor_lst
                for item in neighbor_lst:
                    com_lvl.append(item)
                for i in neighbor_lst:
                    tmp_lst = g.neighbors(i)   
                    for tmp in tmp_lst:
                        if tmp not in com_lvl:
                            children[i]= [] 
                            parents[tmp] = []   
                            parents[tmp].append(i) 
                            children[i].append(tmp)
                            if tmp not in dff:    
                                dff.append(tmp)
                neighbor_lst = dff
                c += 1    
            for other_node in community:
                shortest_paths = nx.all_shortest_paths(g, source=node,target=other_node)
                sh_path[other_node] = len(list(shortest_paths))
            w = len(l_dct)-1
            while w > 0:
                for i in l_dct[w]:
                    n_score[i] = 1  
                    if children.has_key(i):       
                        for child in children[i]:
                            n_score[i] += e_score[(child, i)]
                    parentss = parents[i]  
                    sscore = .0    
                    for j in parentss:
                        sscore += sh_path[j]
                    for a in parentss:
                        nnode_score = sh_path[a] / sscore    
                        e_score[(i, a)] =  nnode_score * n_score[i]       
                w -= 1    
            for (k, v) in e_score.items():
                k = sorted(k, reverse = False)
                tuple_key = (k[0], k[1])
                betw[tuple_key] = 0
                betw[tuple_key] += v


        s_lst = []
        for (k, v) in betw.iteritems():
            s_lst.append((k[0],k[1],v))
        s_lst = sorted(s_lst, key=lambda element: (int(element[0]), int(element[1])))


        for k in range(len(s_lst)):
            s_lst[k] = list(s_lst[k])
            s_lst[k][2] = s_lst[k][2]/2
            s_lst[k] = tuple(s_lst[k])


        gf = open (self.fn1,'w')
        for i in s_lst:
            gf.writelines("(%s,%s,%.1f)\n"%i)
        gf.close()


sc = SparkContext('local','example')
fn1 = sys.argv[-1]
fn2 = sys.argv[-2]
fn3 = sys.argv[-3]
if os.path.exists(fn3):
    rdd_tr = sc.textFile(fn3)

rdd_tr = rdd_tr.mapPartitions(lambda x: csv.reader(x))
def remove_header(itr_index, itr):
    return iter(list(itr)[1:]) if itr_index == 0 else itr
def train_el(items):
    return (items[0],items[1])
rdd_train_or = rdd_tr.mapPartitionsWithIndex(remove_header)
rdd_train_or = rdd_train_or.map(train_el).groupByKey()
prod_rdd = rdd_train_or.cartesian(rdd_train_or).filter(lambda (x,y): x[0] < y[0])
inter_rdd = prod_rdd.map(lambda (x,y): (x[0], y[0], len(set(x[1]).intersection(set(y[1]))))).filter(lambda (x,y,z): z >= 3).map(lambda (x,y,z): (x,y))
users_rdd = rdd_train_or.map(lambda (x,y): (x, ))
node_lst = users_rdd.collect()
edge_lst = inter_rdd.collect()
c = Betweenness(node_lst, edge_lst, fn1)
c.calc_btw()
