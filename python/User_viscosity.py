# -*- coding: utf-8 -*-
"""
Describe: 用户粘性分析
Created By:Yulong Shi
Created Date:2018-09-04
"""
from pyspark.sql import SparkSession
from joblib import Parallel, delayed
from pyspark.sql.types import *
import datetime
import time
import numpy as np
import pandas as pd
from functools import reduce

schema = StructType([StructField("user_id", StringType(), True),
                     StructField("Rein", FloatType(), True),
                     StructField("Rera",FloatType(),True)])

def get_ids(x):
    temp=[i['gap_week'] for i in temp1 if i['userid']==x]
    s_t=sorted(temp)
    s=[s_t[i]-s_t[i-1] for i in range(1,len(s_t))]
    return x,s

def INDICATOR(x):
    s=x[1]
    Rein=1/np.mean(s)
    return x[0],Rein

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

  part_dt=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
  
  sql="""
  SELECT c.userid,
  ceil(datediff(c.date_tm,c.mind)/15) AS gap_week
  FROM(
      SELECT b.origuserid AS userid,
      b.date_tm,
      MIN(b.date_tm) OVER(PARTITION BY b.origuserid) AS mind
      FROM (SELECT origuserid,to_date(clickdate) AS date_tm
            FROM dwd_data.dwd_zx_log_tj_click
            WHERE part_dt='%s' AND origuserid is not null
            ) AS b
      GROUP BY b.origuserid,b.date_tm) AS c
  GROUP BY c.userid,ceil(datediff(c.date_tm,c.mind)/15)
  """%part_dt
  clickdate=spark.sql(sql).rdd.collect()
  temp1=[]
  for x in clickdate:
      temp=x.asDict()
      temp1.append(temp)
  idset=set([temp1[i]['userid'] for i in range(len(temp1))])
  S=Parallel(n_jobs=16, verbose=1, pre_dispatch='2*n_jobs')(delayed(get_ids)(id) for id in idset)
  indicator=Parallel(n_jobs=16, verbose=1, pre_dispatch='2*n_jobs')(delayed(INDICATOR)(i) for i in S)
  results=[]
  for i in indicator:
      result=[i[0],i[1]]
      results.append(result)
  name=['userid','Rein']
  results=pd.DataFrame(columns=name,data=results)
  results.set_index(["userid"],inplace=True)
  
  sql="""
  SELECT c.userid,
  datediff(c.maxd,c.mind)/datediff('%s',c.mind) AS Rera
  FROM(
      SELECT b.origuserid AS userid,
      b.date_tm,
      MAX(b.date_tm) OVER(PARTITION BY b.origuserid) AS maxd,
      MIN(b.date_tm) OVER(PARTITION BY b.origuserid) AS mind
      FROM (SELECT origuserid,to_date(clickdate) AS date_tm
            FROM dwd_data.dwd_zx_log_tj_click
            WHERE part_dt='%s' AND origuserid is not null
            ) AS b
      GROUP BY b.origuserid,b.date_tm) AS c
  GROUP BY c.userid,datediff(c.maxd,c.mind)/datediff('%s',c.mind)
  """%(part_dt,part_dt,part_dt)
  
  Rera=spark.sql(sql).rdd.collect()
  name=['userid','Rera']
  Rera=pd.DataFrame(columns=name,data=Rera)
  Rera.set_index(["userid"],inplace=True)          
  
  activity=pd.concat([results,Rera], axis=1,join='outer')
  sql="""create table IF NOT EXISTS datamining_temp.User_viscosity
      (user_id          string   comment '用户id',
      Rein              float    comment '留存指标',
      Rera              float    comment '召回率指标'
      )partitioned by(part_dt string) row format delimited fields terminated by '\u0001' """
  spark.sql(sql)
  
  df=spark.createDataFrame(activity,schema=schema)
  #df.select("*").show(3)
  df.registerTempTable('df_temp')
  sqldf="""insert overwrite table datamining_temp.User_viscosity partition(part_dt)
        select 
        a.user_id,
        a.Rein,
        a.Rera,
        '%s' as part_dt from df_temp as a"""%part_dt
  spark.sql(sqldf)
  spark.stop()
  