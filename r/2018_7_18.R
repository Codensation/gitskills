m1<-c();max_m1<-max(syl_1$m1_inv_cnt);
for (i in c(1:(max_m1+1))){
  m1[i]=sum(syl_1$m1_inv_cnt==i-1)
}
m1_n=c(m1[1],m1[2],sum(m1[3:4]),sum(m1[5:9]),sum(m1[10:26]),sum(m1[27:51]),sum(m1)-sum(m1[1:51]));

tt=matrix(nrow = 6,ncol = 7);
for (i in c(1:6)){
  #tt_i=list()
  m1<-c()
  max_m<-max(syl_1[,i])
  for (j in c(1:(max_m+1))){
    m1[j]=sum(syl_1[,i]==j-1)
  }
  temp=c(m1[1],m1[2],sum(m1[3:4]),sum(m1[5:9]),sum(m1[10:26]),sum(m1[27:51]),sum(m1)-sum(m1[1:51]))
  tt[i,]=temp
}
mean_1=apply(tt[,1:7],2,mean);
sd_1=apply(tt[,1:7],2,sd);
a=(sd_1/mean_1)/sum(sd_1/mean_1); #变异系数
View(t(a))
