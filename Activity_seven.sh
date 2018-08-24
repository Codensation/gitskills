#!/bin/bash                                                                                                                                                         
##========================================================================================================================
##            Script            :            Activity_seven.sh
##            Auhtor            :            yulong Shi
##            Create Date       :            2018-08-14
##            Description       :            七种行为
##            RunExample        :            
##========================================================================================================================

#PROC_DATE=$1
#TODAY=$2
PROC_DATE=`date +"%Y-%m-%d" -d'-1 day'`
#TODAY=`date +"%Y-%m-%d" -d'-0 day'`
echo $PROC_DATE
#echo $TODAY

#投资
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_invest_info
(        
  user_id            string           comment   '用户id',
  M1_inv_cnt         int              comment   '近1个月投资次数',
  M2_inv_cnt         int              comment   '前第2个月投资次数',
  M3_inv_cnt         int              comment   '前第3个月投资次数',
  M4_inv_cnt         int              comment   '前第4个月投资次数',
  M5_inv_cnt         int              comment   '前第5个月投资次数',
  M6_inv_cnt         int              comment   '前第6个月投资次数',
  M7_inv_cnt         int              comment   '半年之前投资次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_invest_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_inv_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_inv_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_inv_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_inv_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_inv_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_inv_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_inv_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.inv_ppl_id as usr_id
            ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
        from 
         (select inv_ppl_id,to_date(inv_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_INV_RCD
       where part_dt='$PROC_DATE'
       group by inv_ppl_id,to_date(inv_tm)) as b
        ) as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#抢券
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_robsta_info
(             
  user_id            string           comment   '用户id',
  M1_rob_sta_cnt         int              comment   '近1个月抢券次数',
  M2_rob_sta_cnt         int              comment   '前第2个月抢券次数',
  M3_rob_sta_cnt         int              comment   '前第3个月抢券次数',
  M4_rob_sta_cnt         int              comment   '前第4个月抢券次数',
  M5_rob_sta_cnt         int              comment   '前第5个月抢券次数',
  M6_rob_sta_cnt         int              comment   '前第6个月抢券次数',
  M7_rob_sta_cnt         int              comment   '半年之前抢券次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_robsta_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_rob_sta_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_rob_sta_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_rob_sta_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_rob_sta_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_rob_sta_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_rob_sta_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_rob_sta_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.dtrb_usr_id as usr_id
            ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
        from 
         (select dtrb_usr_id,to_date(dtrb_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_RWD_DTRB_RCD
       where part_dt='$PROC_DATE'
        and src in ('神券整点抢','高返神券整点抢') 
        and sts_cd=1	   
       group by dtrb_usr_id,to_date(dtrb_tm)) as b
        ) as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#积分抽奖
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_poirew_info
(             
  user_id            string           comment   '用户id',
  M1_poi_rew_cnt         int              comment   '近1个月积分抽奖次数',
  M2_poi_rew_cnt         int              comment   '前第2个月积分抽奖次数',
  M3_poi_rew_cnt         int              comment   '前第3个月积分抽奖次数',
  M4_poi_rew_cnt         int              comment   '前第4个月积分抽奖次数',
  M5_poi_rew_cnt         int              comment   '前第5个月积分抽奖次数',
  M6_poi_rew_cnt         int              comment   '前第6个月积分抽奖次数',
  M7_poi_rew_cnt         int              comment   '半年之前积分抽奖次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_poirew_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_poi_rew_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_poi_rew_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_poi_rew_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_poi_rew_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_poi_rew_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_poi_rew_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_poi_rew_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.dtrb_usr_id as usr_id
            ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
        from 
         (select dtrb_usr_id,to_date(dtrb_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_RWD_DTRB_RCD
       where part_dt='$PROC_DATE'
        and src in ('小牛积分抽奖','积分抽奖活动','新年积分抽奖','积分抽奖二期','积分抽奖三期') 
        and sts_cd=1	   
       group by dtrb_usr_id,to_date(dtrb_tm)) as b
        ) as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#积分兑换
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_pnt_exg_info
(             
  user_id            string           comment   '用户id',
  M1_pnt_exg_cnt         int              comment   '近1个月积分兑换次数',
  M2_pnt_exg_cnt         int              comment   '前第2个月积分兑换次数',
  M3_pnt_exg_cnt         int              comment   '前第3个月积分兑换次数',
  M4_pnt_exg_cnt         int              comment   '前第4个月积分兑换次数',
  M5_pnt_exg_cnt         int              comment   '前第5个月积分兑换次数',
  M6_pnt_exg_cnt         int              comment   '前第6个月积分兑换次数',
  M7_pnt_exg_cnt         int              comment   '半年之前积分兑换次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_pnt_exg_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_pnt_exg_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_pnt_exg_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_pnt_exg_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_pnt_exg_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_pnt_exg_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_pnt_exg_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_pnt_exg_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.usr_id
            ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
        from 
           (select usr_id,to_date(crt_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_PNT_EXG_RCD
       where part_dt='$PROC_DATE'
       group by usr_id,to_date(crt_tm)) as b)
        as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#收益PK
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_versus_info
(             
  user_id            string           comment   '用户id',
  M1_versus_cnt         int              comment   '近1个月收益PK次数',
  M2_versus_cnt         int              comment   '前第2个月收益PK次数',
  M3_versus_cnt         int              comment   '前第3个月收益PK次数',
  M4_versus_cnt         int              comment   '前第4个月收益PK次数',
  M5_versus_cnt         int              comment   '前第5个月收益PK次数',
  M6_versus_cnt         int              comment   '前第6个月收益PK次数',
  M7_versus_cnt         int              comment   '半年之前收益PK次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_versus_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_versus_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_versus_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_versus_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_versus_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_versus_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_versus_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_versus_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.userid as usr_id
            ,ceil(datediff('$PROC_DATE',b.date_tm)/30) as gap_m 
        from 
       (select userid,to_date(playtime) as date_tm
       from dwd_data.dwd_zx_t_activity_versus_records
       where part_dt='$PROC_DATE'
       group by userid,to_date(playtime)) as b
       ) as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#签到
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_sign_info
(             
  user_id            string           comment   '用户id',
  M1_sgn_pnt_cnt         int              comment   '近1个月签到次数',
  M2_sgn_pnt_cnt         int              comment   '前第2个月签到次数',
  M3_sgn_pnt_cnt         int              comment   '前第3个月签到次数',
  M4_sgn_pnt_cnt         int              comment   '前第4个月签到次数',
  M5_sgn_pnt_cnt         int              comment   '前第5个月签到次数',
  M6_sgn_pnt_cnt         int              comment   '前第6个月签到次数',
  M7_sgn_pnt_cnt         int              comment   '半年之前签到次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_sign_info partition(part_dt)
select c.usr_id
    ,sum(case when c.gap_m=1 then 1 else 0 end) as M1_sgn_pnt_cnt
    ,sum(case when c.gap_m=2 then 1 else 0 end) as M2_sgn_pnt_cnt
    ,sum(case when c.gap_m=3 then 1 else 0 end) as M3_sgn_pnt_cnt
    ,sum(case when c.gap_m=4 then 1 else 0 end) as M4_sgn_pnt_cnt
    ,sum(case when c.gap_m=5 then 1 else 0 end) as M5_sgn_pnt_cnt
    ,sum(case when c.gap_m=6 then 1 else 0 end) as M6_sgn_pnt_cnt
    ,sum(case when c.gap_m=7 then 1 else 0 end) as M7_sgn_pnt_cnt
    , '$PROC_DATE'
from (
    select a.usr_id
        ,case 
            when a.gap_m<=0 then 1
            when a.gap_m>=7 then 7		
            else a.gap_m end as gap_m
    from (
        select b.usr_id
            ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
        from 
        (select usr_id,to_date(crt_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_SGN_PNT_RCD
       where part_dt='$PROC_DATE'
       group by usr_id,to_date(crt_tm)) as b)
	   as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#点击行为替换登录
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_login_info
(             
  user_id            string           comment   '用户id',
  M1_login_flag      int              comment   '近1个月点击登录次数',
  M2_login_flag      int              comment   '前第2个月点击登录次数',
  M3_login_flag      int              comment   '前第3个月点击登录次数',
  M4_login_flag      int              comment   '前第4个月点击登录次数',
  M5_login_flag      int              comment   '前第5个月点击登录次数',
  M6_login_flag      int              comment   '前第6个月点击登录次数',
  M7_login_flag      int              comment   '半年前点击登录次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql1="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_login_info partition(part_dt)
select a.usr_id
    ,sum(case when a.gap_m<=1 then 1 else 0 end) as M1_login_cnt
    ,sum(case when a.gap_m=2 then 1 else 0 end) as M2_login_cnt
    ,sum(case when a.gap_m=3 then 1 else 0 end) as M3_login_cnt
    ,sum(case when a.gap_m=4 then 1 else 0 end) as M4_login_cnt
    ,sum(case when a.gap_m=5 then 1 else 0 end) as M5_login_cnt
    ,sum(case when a.gap_m=6 then 1 else 0 end) as M6_login_cnt
    ,sum(case when a.gap_m>=7 then 1 else 0 end) as M7_login_cnt
    , '$PROC_DATE'	
from (
    select c.usr_id
    ,case 
        when c.gap_m<=0 then 1
        when c.gap_m>=7 then 7		
         else c.gap_m end as gap_m
    from( select b.id as usr_id
           ,ceil(datediff('$PROC_DATE',to_date(b.date_tm))/30) as gap_m 
    from (select id,to_date(clickdate) as date_tm
       from dwd_data.dwd_zx_log_tj_click
       where part_dt='$PROC_DATE'
       group by id,to_date(clickdate))as b)
        as c) as a
group by a.usr_id
;
"

echo $sql1
hive -e "$sql1"



if [ $? -eq 0 ];then
    echo "The whole procedure has run successfully!"
else
    echo "The whole procedure has failed!"
fi