# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 11:16:54 2018

@author: xn088969
"""
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from dateutil.parser import parse
from datetime import timedelta
import pandas as pd
import requests
from geopy.geocoders import Nominatim  
from collections import Counter
import Geohash.geohash as geohash

V_matrix=pd.read_csv('V_matrix.csv',encoding= "utf-8",index_col='out_id')
official_holiday=[datetime(2018,1,1),datetime(2018,2,15),datetime(2018,2,16),datetime(2018,2,17),
                  datetime(2018,2,18),datetime(2018,2,19),datetime(2018,2,20),datetime(2018,2,21),
                  datetime(2018,4,5),datetime(2018,4,6),datetime(2018,4,7),datetime(2018,4,29),
                  datetime(2018,4,30),datetime(2018,5,1),datetime(2018,6,16),datetime(2018,6,17),
                  datetime(2018,6,18),datetime(2018,9,22),datetime(2018,9,23),datetime(2018,9,24),
                  datetime(2018,10,1),datetime(2018,10,2),datetime(2018,10,3),datetime(2018,10,4),
                  datetime(2018,10,5),datetime(2018,10,6),datetime(2018,10,7)]
officail_duty=[datetime(2018,2,24),datetime(2018,4,8),datetime(2018,2,11),datetime(2018,4,28),
               datetime(2018,9,29),datetime(2018,9,30)]


def get_Week_Clock(x):     
    '''输入一个日期字符，判断它是周几和时刻0-7依次表示周日周一到周六和法定节日'''
    date=datetime.strptime(x[0:10], '%Y-%m-%d')
    if date in officail_duty[0:2]:
        week = '1'
    elif date in officail_duty[2:6]:
        week = '5'
    elif date in official_holiday:
        week = '7'
    else:
        week=date.strftime('%w')
    return week,x[11:13]

#def get_outid_v(x):
#    '''输入整个train训练集，计算出每个车id在24小时不同时刻的平均速度，其中用到fun_v函数'''
#    num_x = 0
#    v_id = []
#    for i in x['out_id'].unique():
#        v_24 = []
#        for j in range(24):
#            num_clock = '0%s'%str(j)
#            temp = x[x['clock'] == num_clock[-2:]]
#            D = temp[temp['out_id'] == '%s'%i]
#            v_24.append(fun_v(D))
#        v_id.append(v_24)
#        num_x += 1
#        print('已完成'+'%6.3f'%(100*num_x/len(x['out_id'].unique()))+'%.')
#    return [i for i in x['out_id'].unique()], v_id
#
#def fun_v(x):
#    '''输入同样的车id和同样的时刻数据，根据时间和距离计算速度(单位是m/s)，其中会用到fun_len函数'''
#    v = []
#    for i in range(len(x)):
#        x_row = x.iloc[i,:]
#        t = parse(x_row['end_time']) - parse(x_row['start_time'])
#        t_seconds = t.days*24*3600 + t.seconds
#        s_len = fun_len(x_row['start_lat'], x_row['start_lon'], x_row['end_lat'], x_row['end_lon'])
#        v_temp = float(s_len/t_seconds)
#        v.append(v_temp)
#    return np.mean(v)
#
#不需要预测时间
#def fun_time(x):
#    '''输入同样的车id和同样的时刻数据，计算平均时间'''
#    time = []
#    for x_row in x:
#        t = parse(x_row['end_time']) - parse(x_row['start_time'])
#        t_seconds = t.days*24*3600 + t.seconds
#        time.append(t_seconds)
#    return np.mean(time)


def fun_len(start_lat, start_lon, end_lat, end_lon):
    '''输入起始点和终点经纬度，计算他们的实际距离，单位是米'''
    start_lat, start_lon, end_lat, end_lon = map(radians, [start_lat, start_lon, end_lat, end_lon])
    dlon = end_lon - start_lon 
    dlat = end_lat - start_lat
    a = sin(dlat/2)**2 + cos(start_lat) * cos(end_lat) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371
    return c * r * 1000

def fun_predict(x, x_train, epsilon = 100, strong = 1): #epsilon 是初始地点范围，单位是米
    global address
    if strong == 1:       
        week, clock = get_Week_Clock(x['start_time'])
        state = address.loc[x['out_id'],'state']
        temp = x_train[(x_train['state'] == state) & (x_train['week'] == week) & (x_train['clock'] == clock)] 
    else:
        week, clock = get_Week_Clock(x['start_time'])
        city = address.loc[x['out_id'],'city']
        temp = x_train[(x_train['city'] == city) & (x_train['week'] == week)]
    if not temp.empty:
        neighbor = []
        for i in range(len(temp)):
            sample = temp.iloc[i,:]
            if fun_len(x['start_lat'], x['start_lon'], sample['start_lat'], sample['start_lon']) <= epsilon:
                neighbor.append(sample)
        if neighbor:
            predict_lat = np.mean([j['end_lat'] for j in neighbor])
            predict_lon = np.mean([j['end_lon'] for j in neighbor])
        else:
            predict_lat = 0
            predict_lon = 0
    else:
        predict_lat = 0
        predict_lon = 0
    return predict_lat, predict_lon

def fun_history_predict(x, x_train):
    global address
    predict_lat = []
    predict_lon = []
    num_x = 0
    for i in x['out_id']:
        temp=train[train['out_id']==i]
        end_lat = np.median(temp['end_lat'])
        end_lon = np.median(temp['end_lon'])
        predict_lat.append(end_lat)
        predict_lon.append(end_lon)
        num_x += 1
        if num_x%1000 == 0:
            print('已完成'+'%6.3f'%(100*num_x/len(x))+'%.')
    return predict_lat, predict_lon
    

def predict(test,train,strong = 1): 
    global address
    predict_lat = []
    predict_lon = []
    num_x = 0
    for i in range(len(test)):
        end_lat, end_lon = fun_predict(test.iloc[i,:], train, strong = strong)
        predict_lat.append(end_lat)
        predict_lon.append(end_lon)
        num_x += 1
        if num_x%1000 == 0:
            print('已完成'+'%6.3f'%(100*num_x/len(test))+'%.')
    return predict_lat, predict_lon

def fun_getdistrict(lat,lon):
    items = {'location': '%s,%s'%(lat,lon), 'ak': '1EcCifMvl6d7GR4huZbT0qh7rpND1QDx', 'output': 'json'}
    res = requests.get('http://api.map.baidu.com/geocoder/v2/', params=items)
    result = res.json()
    city = result['result']['addressComponent']['city'] 
    district = result['result']['addressComponent']['district']
    return city,district

def fun_getstate(lat,lon):
    '''有问题'''
    geolocator = Nominatim()
    location = geolocator.reverse('%s,%s'%(lat,lon))   
    try:
        state = location.raw['address']['state']
        city = location.raw['address']['city']
    except KeyError as e:
        try:
            city = location.raw['address']['city_district']
        except KeyError as e:
            try:
                city = location.raw['address']['state_district']
            except KeyError as e:
                city = 'nan'
    return state,city
    
specific_id = []                
def fun_getaddress(id, method = 'Baiduapi'):
    global train, specific_id
    temp = train[train['out_id'] == id]
    l = temp[temp['week'] == '3']
    try:        
        if method == 'Geopy':
            state, city = fun_getstate(l['start_lat'][0],l['start_lon'][0])
        elif method == 'Baiduapi':
            state, city = fun_getdistrict(l['start_lat'][0],l['start_lon'][0]) 
    except IndexError as e:
        state, city = 'nan', 'nan'
        specific_id.append(id)
    return id,state,city

#lat=l['start_lat'][0]
#lon=l['start_lon'][0]
def add_address(train, address):
    state = []
    for i in train['out_id']:
        temp = address.loc[i,'state']
        state.append(temp)
    train['state'] = state
    return print('pass')
def add_city(train,address):
    city = []
    for i in train['out_id']:
        temp = address.loc[i,'city']
        city.append(temp)
    train['city'] = city
    return print('pass')

def fun_gethabit(id):
    temp = train[train['out_id'] == id]
    temp_week = Counter(temp['week']).most_common()[0][0]
    temp_clock = Counter(temp['clock']).most_common()[0][0]
    temp_len = len(temp)
    
def geoEncoding(data, precision):
    temp = data[['start_lat','start_lon']]
    geohashList = []
    for i in temp.values:
        geohashList.append(geohash.encode(i[0], i[1], precision))
    data['geohash{}'.format(precision)] = geohashList
    return data

def getError(data):
    temp = data[['end_lat','end_lon','predict_end_lat','predict_end_lon']].astype(float)
    error = []
    for i in temp.values:
        t = fun_len(i[0], i[1], i[2], i[3])
        error.append(t)
    return np.sum(np.array(error)) / temp.shape[0]

    #A=Counter(train['clock'])















