 # -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def 


g=nx.Graph()
path="E:/8月第四周/Hamsterster.txt"
g_net=np.loadtxt(path,delimiter=' ')
g.add_edges_from(g_net)
#g.number_of_nodes()
#g.number_of_edges()
a=sorted(g.degree,key=lambda x: x[1],reverse=True)
a_v=list(dict(a).values())
plt.figure(1) ##汇制度分布图
d={}
for item in set(a_v):
    d[item]=a_v.count(item)
plt.loglog(list(d.keys()),list(d.values()),'b.')
plt.xlim(0,1000);plt.ylim(0,1000)

g_adj=(g.adj)
def Expansion(g,v,P):
    #G is a graph,P is a path
    P.ne/len(P)
    
    return
def Neighbor(g,p):
    

p={};
for i in np.arange(1,g.number_of_nodes()+1):
    p[i]=i;v=i;Exp=1;temp=1
    while len(temp)!=0
    temp=[jtem for jtem in Neighbor(g,p[i])) and Expansion(g,jtem,p[i])>Exp]
    p[i]=p[i].append(temp)
    Exp=max([Expansion(g,jtem,p) for jtem in Neighbor(g,p)])
    
    
        
    g.adj[item]
