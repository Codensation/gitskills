setwd("C:/Users/xn088969/Desktop")
syl<-read.table("shiyulong.csv",header = TRUE,row.names = "user_id",sep=",");
install.packages("dplyr");
library("dplyr");
syl_1=filter(syl,m1_inv_cnt!=0|m2_inv_cnt!=0|m3_inv_cnt!=0|m4_inv_cnt!=0|m5_inv_cnt!=0|m6_inv_cnt!=0);
mean_1=apply(syl_1[,1:6],2,mean);
sd_1=apply(syl_1[,1:6],2,sd);
a=(sd_1/mean_1)/sum(sd_1/mean_1); #变异系数

lm_6<-lm(m6_inv_cnt~1+m1_inv_cnt+m2_inv_cnt+m3_inv_cnt+m4_inv_cnt+m5_inv_cnt,data=syl_1);
 summary(lm_6)

m2_fre<-c();max_m1<-max(syl_1$m2_inv_cnt);
for (i in c(1:(max_m1+1))){
  m2_fre[i]=sum(syl_1$m2_inv_cnt==i-1)
}
View(m2_fre)
plot(m1_fre);

b<-apply(syl_1[,1:6],1,mean);
