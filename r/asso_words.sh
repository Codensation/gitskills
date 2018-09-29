#!/bin/bash 
#PROGRAM NAME :YQ_NEWS_words_association.sh
#DESCRIPTION  :词关联模型脚本
#EXPLAIN      :
#PROGRAMMER   :leixin
#DATE WRITTEN :2017-07-24
###################################################################################################################

TODAY=`date +"%Y-%m-%d" -d' 0 day'`
OUTPUT_DIR="/home/leixin/js_ciyun.txt"
echo $OUTPUT_DIR

sql0="use datamining_temp;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;
create table IF NOT EXISTS  data_mining.yq2_news_rule_wordcloud(
rule_json        string comment '字符串',
plt_name  string comment '平台名称',
sentiment string comment '情感') comment 'NLP_关联词云结果表' partitioned by(dat_dt string) row format delimited fields terminated by '\u0001' ;"

echo $sql0
#hive -e "${sql0}"

Rscript /home/leixin/yq2_asso_parallel.R
sql2="SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET mapreduce.job.queuename=root.datamining;
SET hive.map.aggr = false;
LOAD DATA LOCAL INPATH '/home/leixin/js_ciyun.txt' OVERWRITE into table data_mining.yq2_news_rule_wordcloud partition(dat_dt='${TODAY}');"

echo $sql2
hive -e "${sql2}"


