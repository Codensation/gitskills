# -*- coding: utf-8 -*-
"""
Describe: 计算得分
Created By:Yulong Shi
Created Date:2018-08-09
"""
import scipy.stats as stats
from pyspark.sql import SparkSession
from joblib import Parallel, delayed
from pyspark.sql.types import *
import datetime
import time
import numpy as np
import pandas as pd
from functools import reduce
s=[10,8,6,6,4,2,1];
w=[10,8,6,4,2,1];

schema = StructType([StructField("user_id", StringType(), True),
                     StructField("activity_score", FloatType(), True),
                     StructField("activity_label", StringType(), True)])
part_dt=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')   ###一天以前

def scale(col):
    s_col=[i/sum(col) for i in col]
    return s_col

def score2label(num):
    num=float('%.6f' %num)
    #print(num)
    if num==0.0:
        return '不活跃'
    elif num>0.0 and num<=1.0:
        return '一级活跃'
    elif num>1.0 and num<=5.0:
        return '二级活跃'
    elif num>5.0 and num<=20.0:
        return '三级活跃'
    elif num>20.0 and num<=100:
        return '四级活跃'
    else: return '无效得分'                                                   

if __name__ == "__main__":
  spark = SparkSession.builder \
        .master("yarn") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.sql.shuffle.partitions", 800) \
        .appName("neighbors_black_list") \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config('hive.exec.max.dynamic.partitions', 100000) \
        .config('hive.exec.max.dynamic.partitions.pernode', 100000) \
        .getOrCreate()

w=scale(w);                                              ###输出每月的权重


SQL="select * from data_mining.t_zx_invest_info where part_dt=\'"+part_dt+"\'"
inv=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_robsta_info where part_dt=\'"+part_dt+"\'"
rob=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_poirew_info where part_dt=\'"+part_dt+"\'"
rew=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_pnt_exg_info where part_dt=\'"+part_dt+"\'"
exg=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_robsta_info where part_dt=\'"+part_dt+"\'"
sus=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_sign_info where part_dt=\'"+part_dt+"\'"
sig=spark.sql(SQL).toPandas();
SQL="select * from data_mining.t_zx_login_info where part_dt=\'"+part_dt+"\'"
log=spark.sql(SQL).toPandas();
W0=pd.DataFrame(np.dot(inv.as_matrix()[:,1:7],w),index=inv['user_id']);
W1=pd.DataFrame(np.dot(rob.as_matrix()[:,1:7],w),index=rob['user_id']);
W2=pd.DataFrame(np.dot(rew.as_matrix()[:,1:7],w),index=rew['user_id']);
W3=pd.DataFrame(np.dot(exg.as_matrix()[:,1:7],w),index=exg['user_id']);
W4=pd.DataFrame(np.dot(sus.as_matrix()[:,1:7],w),index=sus['user_id']);
W5=pd.DataFrame(np.dot(sig.as_matrix()[:,1:7],w),index=sig['user_id']);
W6=pd.DataFrame(np.dot(log.as_matrix()[:,1:7],w),index=log['user_id']);
#pd.concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,
#      keys=None, levels=None, names=None, verify_integrity=False)
ACTION_W = pd.concat([W0,W1,W2,W3,W4,W5,W6], axis=1,join='outer')
score_data=pd.DataFrame(np.dot(ACTION_W.fillna(0).as_matrix(),s),index=ACTION_W.index)   ###输出得分
temp=score_data/max(score_data[0])*100
label=temp.apply(score2label,axis = 1)
score_data=pd.concat([score_data,label],axis=1)
data_x=np.array(score_data)
data_y=np.array(score_data.index)
data_x=data_x.tolist()
data_y=data_y.tolist()

for i in range(len(data_x)):
    data_x[i].insert(0,data_y[i])
    
sql="""
create table IF NOT EXISTS datamining_temp.activity_score_data
(user_id          string   comment '用户id',
activity_score    float    comment '行为得分',
activity_label    string   comment '行为活跃度等级标签'
)partitioned by(part_dt string) row format delimited fields terminated by '\u0001' 
"""
spark.sql(sql)

df=spark.createDataFrame(data_x,schema=schema)
#df.select("*").show(3)
df.registerTempTable('df_temp')
sqldf="""
insert overwrite table datamining_temp.activity_score_data partition(part_dt)
select 
a.user_id,
a.activity_score,
a.activity_label,
'%s' as part_dt from df_temp as a
"""%part_dt
spark.sql(sqldf)
spark.stop()


