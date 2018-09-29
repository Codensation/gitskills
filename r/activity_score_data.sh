#!/bin/bash 
#PROGRAM NAME :activity_score_data.sh
#DESCRIPTION  :用户行为得分
#EXPLAIN      :
#PROGRAMMER   :shiyulong
#DATE WRITTEN :2017-07-24
###################################################################################################################

PROC_DATE=`date +"%Y-%m-%d" -d'-1 day'`
echo $PROC_DATE
OUTPUT_DIR="/home/shiyulong/score_data.txt"
echo $OUTPUT_DIR

sql0="
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;
create table IF NOT EXISTS datamining_temp.activity_score_data
(
user_id           string   comment '用户id',
activity_score    int      comment '行为得分',
activity_label    string   comment '行为活跃度等级标签'
)partitioned by(part_dt string)
row format delimited fields terminated by '\u0001' ;
"

echo $sql0
hive -e "$sql0"

Rscript /home/shiyulong/activity_score_data.R
sql1=
"SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;
LOAD DATA LOCAL INPATH '/home/shiyulong/score_data.txt' OVERWRITE into table datamining_temp.activity_score_data partition(part_dt='$PROC_DATE');
"

echo $sql1
hive -e "${sql1}"

if [ $? -eq 0 ];then
    echo "The whole procedure has run successfully!"
else
    echo "The whole procedure has failed!"
fi
