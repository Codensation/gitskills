#!/bin/bash                                                                                                                                                         
##========================================================================================================================
##            Script            :            01_zx_basic_data.sh
##            Auhtor            :            Chanejie Fu
##            Create Date       :            2018-07-06
##            Description       :            
##            RunExample        :            
##========================================================================================================================

#PROC_DATE=$1
#TODAY=$2
PROC_DATE=`date +"%Y-%m-%d" -d'-1 day'`
#TODAY=`date +"%Y-%m-%d" -d'-0 day'`
echo $PROC_DATE
#echo $TODAY

#投资记录表
sql0="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_inv_cnt_info
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

insert overwrite table data_mining.t_zx_inv_cnt_info partition(part_dt)
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
        select inv_ppl_id as usr_id
            ,ceil(datediff('$PROC_DATE',to_date(inv_tm))/30) as gap_m 
        from dwd_data.DWD_EVT_ZX_INV_RCD
        where part_dt='$PROC_DATE' ) as a ) as c
group by c.usr_id
;
"

echo $sql1
hive -e "$sql1"

#投资记录表
sql2="
set mapred.job.queue.name=datamining;
create table if not exists data_mining.t_zx_inv_cnt_1_info
(             
  user_id            string           comment   '用户id',
  M1_inv_cnt         int              comment   '注册1个月内投资次数',
  M2_inv_cnt         int              comment   '注册2个月内投资次数',
  M3_inv_cnt         int              comment   '注册3个月内投资次数',
  M4_inv_cnt         int              comment   '注册4个月内投资次数',
  M5_inv_cnt         int              comment   '注册5个月内投资次数',
  M6_inv_cnt         int              comment   '注册6个月内投资次数',
  M7_inv_cnt         int              comment   '注册半年之后投资次数'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql2
hive -e "$sql2"

sql3="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;

insert overwrite table data_mining.t_zx_inv_cnt_1_info partition(part_dt)
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
        select t1.inv_ppl_id as usr_id
            ,t2.reg_tm
            ,t1.inv_tm
            ,ceil(datediff(to_date(t1.inv_tm),to_date(t2.reg_tm))/30) as gap_m 
        from dwd_data.DWD_EVT_ZX_INV_RCD as t1
        left join dwd_data.DWD_CUT_ZX_USR_BAS_INF as t2
        on t1.inv_ppl_id=t2.usr_id and t2.part_dt='$PROC_DATE'
        where t1.part_dt='$PROC_DATE' ) as a
    group by a.gap_m
    order by a.gap_m;
    ) as c
group by c.usr_id
;
"

echo $sql3
hive -e "$sql3"

if [ $? -eq 0 ];then
    echo "The whole procedure has run successfully!"
else
    echo "The whole procedure has failed!"
fi
