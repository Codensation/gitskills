# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 17:32:04 2018

@author: xn088969
"""


import numpy as np
import pandas as pd
import os
os.chdir('E:/python3code/car_predict/')
import datetime
from dateutil.parser import parse
from collections import Counter
from fun import *


train = pd.read_csv('train_new.csv',encoding= "utf-8",index_col='r_key')
train['out_id'] = train['out_id'].apply(str)  #将id列的所有数据转化成字符，因为有的不是字符


''' 重塑数据   '''   
'''时间重塑''' 
temp = [get_Week_Clock(i) for i in train['start_time']]
train['week'] = [i[0] for i in temp]
train['clock'] = [i[1] for i in temp]
'''地点重塑'''
address = []
for i in train['out_id'].unique():
    temp = fun_getaddress(i)
    address.append(temp)
    
add_address(train, address)
add_city(train,address)

train = geoEncoding(train, 12) #直接用geohash编码

'''
city_district = []
for i in range(len(train)):
    sample = train.iloc[i,:]
    temp = fun_getstate(sample['start_lat'],sample['start_lon'])
    city_district.append(temp)
'''

address=pd.read_csv('address2.csv',encoding= "utf-8",index_col='out_id')


address2 = pd.DataFrame(address,columns=['out_id','state','city'])
address2.to_csv('address2.csv', index = False)
train['city'] = [for i[0] in city_district]
train['district'] = [for i[1] in city_district]
train.to_csv('train_v1.csv',index=True,encoding= "utf-8")

result = predict(test,train)
results = pd.DataFrame()
results['r_key'] = test.index
results['out_id'] = test['out_id']
results['end_lat'] = result[0]
results['end_lon'] = result[1]
results.to_csv('results.csv', index = False)

test2 = results[results['end_lat'] == 0]
testpre2 = test.loc[test2.index, :]
result2 =  predict(testpre2, train, strong = 0)
results.loc[test2.index, 'end_lat'] = result2[0]
results.loc[test2.index, 'end_lon'] = result2[1]

test3 = results[results['end_lat'] == 0]
testpre3 = test.loc[test3.index, :]
result3 = fun_history_predict(testpre3, train)
results.loc[test3.index, 'end_lat'] = result3[0]
results.loc[test3.index, 'end_lon'] = result3[1]

results.to_csv('results.csv', index = False)
"""
'''计算速度矩阵'''
out_id,v_matrix = get_outid_v(train)
V_matrix = pd.DataFrame(data = v_matrix, columns = [str(i) for i in range(24)])
V_matrix['out_id'] = out_id
#V_matrix.to_csv('V_matrix.csv',index=False)
#V_matrix=pd.read_csv('V_matrix.csv',encoding= "utf-8",index_col='out_id')
"""
end_lat, end_lon, end_time= predict(x, train)
test = pd.read_csv('test_new.csv',encoding= "utf-8",index_col='r_key')

results['']


lat,lon,time = predict(test,train)
submit = pd.DataFrame()
submit['r_key'] = test['r_key']
submit['end_lat'] = lat
submit['end_lon'] = lon
submit.to_csv('result.csv', index = False)  




#part_dt=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') 
#a = pd.DataFrame(v_matrix)
#V_matrix.to_csv('V_matrix.csv',index=False)
#A=Counter(train['week'])
#A=Counter(train['clock'])
