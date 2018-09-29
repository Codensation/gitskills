#install.packages("RJDBC")
library(RJDBC)
options(stringsAsFactors = F)
options( java.parameters = '-Xmx16g')
if (Sys.info()['sysname']=="Windows"){
  hive_home="D:/Program/jar/"
  hadoop_home="D:/Program/jar/"
} else {
  hive_home='/appcom/service/hive/lib/'
  hadoop_home='/appcom/service/hadoop/share/hadoop/common/'
}
cp = c( paste0(hadoop_home,"hadoop-common-2.6.0-cdh5.7.0.jar"),
        paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0.jar"),
        paste0(hive_home,"libthrift-0.9.2.jar"),
        paste0(hive_home,"hive-service-1.1.0-cdh5.7.0.jar"),
        paste0(hive_home,"httpclient-4.2.5.jar"),
        paste0(hive_home, "httpcore-4.2.5.jar"),
        paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0-standalone.jar"))

.jinit(classpath = cp)
drv <-JDBC("org.apache.hive.jdbc.HiveDriver", paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0-standalone.jar"))
conn <-dbConnect(drv,"jdbc:hive2://10.8.34.2:10000/datamining_temp","shiyulong","Xn!156517")
dbSendUpdate(conn, "SET mapreduce.job.queuename=root.datamining")
dbSendUpdate(conn,"SET hive.exec.dynamic.partition=true")
dbSendUpdate(conn,"SET hive.exec.dynamic.partition.mode=nonstrict")
dbSendUpdate(conn,"SET hive.exec.max.dynamic.partitions=100000")
dbSendUpdate(conn,"SET hive.exec.max.dynamic.partitions.pernode=100000")
dbSendUpdate(conn,"SET hive.map.aggr = false")
dbSendUpdate(conn,"SET hive.exec.parallel=true")
dbSendUpdate(conn,"SET hive.exec.parallel.thread.number=16")
SQL=paste('select * from datamining_temp.activity where part_dt=\'',Sys.Date()-1,'\'',sep='')
w=dbGetQuery(conn,SQL);
w$activity.gap_m=as.numeric(w$activity.gap_m);
w=w[which(w$activity.gap_m>=0 & w$activity.gap_m<7),c(2:7)];
w[is.na(w)]=0;
w_temp=w[1,]+w[2,];w_temp[c(2:6),]=w[c(2:6),];
w_temp=as.matrix(w_temp);
sum_w=apply(w_temp,2,sum);
for (i in c(1:length(sum_w))){
w_temp[,i]=w_temp[,i]/sum_w[i]}   
w_temp1=apply(w_temp,1,sum);
w=w_temp1/sum(w_temp1);           ###w是最终的权重（列向量）
s=c(10,6,6,4,2,1);                ###行为类型得分
SQL=paste('select * from data_mining.t_zx_robsta_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
rob=dbGetQuery(conn,SQL);
SQL=paste('select * from data_mining.t_zx_poirew_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
rew=dbGetQuery(conn,SQL);
SQL=paste('select * from data_mining.t_zx_pnt_exg_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
exg=dbGetQuery(conn,SQL);
SQL=paste('select * from data_mining.t_zx_versus_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
sus=dbGetQuery(conn,SQL);
SQL=paste('select * from data_mining.t_zx_sign_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
sig=dbGetQuery(conn,SQL);
SQL=paste('select * from data_mining.t_zx_login_info where part_dt=\'',Sys.Date()-1,'\'',sep='');
log=dbGetQuery(conn,SQL);
UID=union(union(union(union(union(rob[,1],rew[,1]),exg[,1]),sus[,1]),sig[,1]),log[,1]);
rob6=as.matrix(rob[,2:7]);rew6=as.matrix(rew[,2:7]);exg6=as.matrix(exg[,2:7]);
sus6=as.matrix(sus[,2:7]);sig6=as.matrix(sig[,2:7]);log6=as.matrix(log[,2:7]);
id=rob[,1];rob_w=rob6%*%w;W1=data.frame(id,rob_w);
id=rew[,1];rew_w=rew6%*%w;W2=data.frame(id,rew_w);
id=exg[,1];exg_w=exg6%*%w;W3=data.frame(id,exg_w);
id=sus[,1];sus_w=sus6%*%w;W4=data.frame(id,sus_w);
id=sig[,1];sig_w=sig6%*%w;W5=data.frame(id,sig_w);
id=log[,1];log_w=log6%*%w;W6=data.frame(id,log_w);
ACTION_W=merge(merge(merge(merge(merge(W1,W2,by='id',all=T),W3,by='id',all=T),W4,by='id',all=T),W5,by='id',all=T),W6,by='id',all=T);
ACTION_W[is.na(ACTION_W)]<-0;
SCORE<- as.matrix(ACTION_W[,2:7])%*%s;
score_data=data.frame(ACTION_W$id,SCORE);
label <- c()
label[which(score_data[,2]==0)] <- '不活跃'
label[which(score_data[,2]>0 & which(score_data[,2]<=10)] <- '一级活跃'
label[which(score_data[,2]>10 & which(score_data[,2]<=50)] <- '二级活跃'
label[which(score_data[,2]>50 & which(score_data[,2]<=100)] <- '三级活跃'
label[which(score_data[,2]>100)] <- '四级活跃'
label <- data.frame(ACTION_W$id，label);
score_data=merge(score_data,label,by='id',all=T); ####最终的表


write.table(score_data,file = 'score_data.txt',sep = '\u0001',row.names = F,col.names = F);

