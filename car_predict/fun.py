# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 11:16:54 2018

@author: xn088969
"""
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from dateutil.parser import parse

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

def get_outid_v(x):
    '''输入整个train训练集，计算出每个车id在24小时不同时刻的平均速度，其中用到fun_v函数'''
    num_x = 0
    v_id = []
    for i in x['out_id'].unique():
        v_24 = []
        for j in range(24):
            temp = x[x['clock'] == '%s'%str(j)]
            D = temp[temp['out_id'] == '%s'%i]
            v_24.append(fun_v(D))
        v_id.append(v_24)
        num_x += 1
        print('已完成'+'%6.3f'%(100*num_x/len(x['out_id'].unique()))+'%.')
    return [i for i in x['out_id'].unique()], v_id

def fun_v(x):
    '''输入同样的车id和同样的时刻数据，根据时间和距离计算速度(单位是m/s)，其中会用到fun_len函数'''
    v = []
    for i in range(len(x)):
        x_row = x.iloc[i,:]
        t = parse(x_row['end_time']) - parse(x_row['start_time'])
        t_seconds = t.days*24*3600 + t.seconds
        s_len = fun_len(x_row['start_lat'], x_row['start_lon'], x_row['end_lat'], x_row['end_lon'])
        v_temp = float(s_len/t_seconds)
        v.append(v_temp)
    return np.mean(v)

def fun_len(start_lat, start_lon, end_lat, end_lon):
    '''输入起始点和终点经纬度，计算他们的实际距离，单位是米'''
    start_lat, start_lon, end_lat, end_lon = map(radians, [start_lat, start_lon, end_lat, end_lon])
    dlon = end_lon - start_lon 
    dlat = end_lat - start_lat
    a = sin(dlat/2)**2 + cos(start_lat) * cos(end_lat) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371
    return c * r * 1000