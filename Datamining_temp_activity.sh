#!/bin/bash                                                                                                                                                         
##========================================================================================================================
##            Script            :            datamining_temp.activity.sh
##            Auhtor            :            yulong Shi
##            Create Date       :            2018-08-08
##            Description       :            
##            RunExample        :            
##========================================================================================================================

#PROC_DATE=$1
#TODAY=$2

PROC_DATE=`date +"%Y-%m-%d" -d'-1 day'`
echo $PROC_DATE

sql0="
create table if not exists datamining_temp.activity
(             
  gap_m            string             comment   '间隔月数',
  cnt_log            int              comment   '登录',
  cnt_sig            int              comment   '签到',
  cnt_sus            int              comment   'pk',
  cnt_exg            int              comment   '兑换',
  cnt_rew            int              comment   '抽奖',
  cnt_rob            int              comment   '抢券'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001'; 
"

echo $sql0
hive -e "$sql0"

sql="
set mapred.job.queue.name=datamining;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false; 
insert overwrite table datamining_temp.activity partition(part_dt)
select
t1.gap_m,
t1.cnt_log,
t2.cnt_sig,
t3.cnt_sus,
t4.cnt_exg,
t5.cnt_rew,
t6.cnt_rob,
'$PROC_DATE'
from
(select f.gap_m,count(*) as cnt_log
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select userid,to_date(clickdate) as date_tm
       from dwd_data.dwd_zx_log_tj_click
       where part_dt='$PROC_DATE'
       group by userid,to_date(clickdate)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m) as t1
left join
(select f.gap_m,count(*) as cnt_sig
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select usr_id,to_date(crt_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_SGN_PNT_RCD
       where part_dt='$PROC_DATE'
       group by usr_id,to_date(crt_tm)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m) as t2
on t1.gap_m=t2.gap_m
left join
(select f.gap_m,count(*) as cnt_sus
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select userid,to_date(playtime) as date_tm
       from dwd_data.dwd_zx_t_activity_versus_records
       where part_dt='$PROC_DATE'
       group by userid,to_date(playtime)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m) as t3
on t1.gap_m=t3.gap_m
left join
(select f.gap_m,count(*) as cnt_exg
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select usr_id,to_date(crt_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_PNT_EXG_RCD
       where part_dt='$PROC_DATE'
       group by usr_id,to_date(crt_tm)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m) as t4
on t1.gap_m=t4.gap_m
left join
(select f.gap_m,count(*) as cnt_rew
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select dtrb_usr_id,to_date(dtrb_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_RWD_DTRB_RCD
       where part_dt='$PROC_DATE'
        and src in ('小牛积分抽奖','积分抽奖活动','新年积分抽奖','积分抽奖二期','积分抽奖三期') 
        and sts_cd=1	   
       group by dtrb_usr_id,to_date(dtrb_tm)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m)as t5
on t1.gap_m=t5.gap_m
left join
(select f.gap_m,count(*) as cnt_rob
from (
    select a.date_tm
          ,ceil(datediff(to_date('$PROC_DATE'),to_date(a.date_tm))/30) as gap_m 
        from 
          (select dtrb_usr_id,to_date(dtrb_tm) as date_tm
       from dwd_data.DWD_EVT_ZX_RWD_DTRB_RCD
       where part_dt='$PROC_DATE'
        and src in ('神券整点抢','高返神券整点抢') 
        and sts_cd=1	   
       group by dtrb_usr_id,to_date(dtrb_tm)) as a
      ) as f
    group by f.gap_m
    order by f.gap_m) as t6
on t1.gap_m=t6.gap_m;"

echo $sql
hive -e "$sql"

if [ $? -eq 0 ];then
    echo "The whole procedure has run successfully!"
else
    echo "The whole procedure has failed!"
fi






