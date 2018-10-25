# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 17:32:04 2018

@author: xn088969
"""

import numpy as np
import pandas as pd
import os
from datetime import datetime
from dateutil.parser import parse
from collections import Counter

os.chdir('E:/python3code/car_predict/')
train=pd.read_csv('train.csv',encoding= "utf-8",index_col='r_key')
    
temp = [get_Week_Clock(i) for i in train['start_time']]
train['week'] = [i[0] for i in temp]
train['clock'] = [i[1] for i in temp]



A=Counter(train['week'])
A=Counter(train['clock'])
