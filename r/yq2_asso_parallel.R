library(RJDBC)
library(arules)
#library(arulesViz)
#library(igraph)
library(jsonlite)
library(parallel)
library(doParallel)
library(foreach)
#library(visNetwork)
#setwd('/home/leixin')
options(stringsAsFactors = F)
options(fileEncoding = 'UTF-8')
options(java.parameters = '-Xmx16g')

#### hadoop hive jar包路径
hive_home='/appcom/service/hive/lib/'
hadoop_home='/appcom/service/hadoop/share/hadoop/common/'

cp = c( paste0(hadoop_home,"hadoop-common-2.6.0-cdh5.7.0.jar"),
        paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0.jar"),
        paste0(hive_home,"libthrift-0.9.2.jar"),
        paste0(hive_home,"hive-service-1.1.0-cdh5.7.0.jar"),
        paste0(hive_home,"httpclient-4.2.5.jar"),
        paste0(hive_home, "httpcore-4.2.5.jar"),
        paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0-standalone.jar"))

.jinit(classpath = cp)
drv <-JDBC("org.apache.hive.jdbc.HiveDriver", paste0(hive_home,"hive-jdbc-1.1.0-cdh5.7.0-standalone.jar"))
conn <-dbConnect(drv,"jdbc:hive2://10.8.34.2:10000/datamining_temp","qinxingde","3.141516")
dbSendUpdate(conn, "SET mapreduce.job.queuename=root.datamining")

sent_words = dbGetQuery(conn, 'select word,sentiment from data_mining.dict_sent')
pos_words = sent_words[, 1][sent_words$sentiment == 1]
neg_words = sent_words[, 1][sent_words$sentiment == -1]

pubdate = as.character(Sys.Date() - 90)
sql = paste0("select upper(t1.content_words_sent) as content_words_keep ,
              upper(t2.plt_mult_name) as plt_mult_name,
              upper(t2.plt_name) as plt_name,t2.sentiment from data_mining.yq2_news_seg as t1 ,
              data_mining.yq2_news_resu as t2 where t1.id=t2.id and t2.pubdate>'",pubdate,
              "' and length(t2.plt_mult_name)>0")
txt = dbGetQuery(conn, sql)
#load('txt.rda')
colnames(txt) <-c("content_words_keep", "plt_mult_name","plt_name","sentiment")
plt_list = dbGetQuery(conn, "select upper(plt_name) as plt_name,upper(pattern) as pattern from data_mining.yq_plt_name")
plt_list=plt_list[!plt_list$plt_name=='ALL',]

plt_list2<-sort(table(txt$plt_name),decreasing = T)
plt_list2<-plt_list2[plt_list2>1]
plt_list3<-plt_list[plt_list$plt_name %in% names(plt_list2),]
plt_list<-rbind(plt_list[plt_list$plt_name=='ALL_XIAONIU',],plt_list3)

remove_words <- function(x) {
  doc = as.vector(x)
  doc <- strsplit(doc, ' ')
  for (i in 1:length(doc))
  {
    doc[[i]] <- doc[[i]][nchar(doc[[i]]) > 1]
  }
  doc
}

remove_words_neg <- function(x) {
  doc = as.vector(x)
  doc <- strsplit(doc, ' ')
  for (i in 1:length(doc))
  {
    doc[[i]] <- doc[[i]][nchar(doc[[i]]) > 1]
    doc[[i]] <- doc[[i]][!doc[[i]] %in% pos_words]
  }
  doc
}

remove_words_pos <- function(x) {
  doc = as.vector(x)
  doc <- strsplit(doc, ' ')
  for (i in 1:length(doc))
  {
    doc[[i]] <- doc[[i]][nchar(doc[[i]]) > 1]
    doc[[i]] <- doc[[i]][!doc[[i]] %in% neg_words]
  }
  doc
}


produce_rules = function(doc) {
  if (2 / length(doc) * 1.25 < 0.05) {
    supp = 0.05
  } else{
    if (length(doc) < 6) {
      supp = 0.4
    }
    else{
      supp = 2 / length(doc) * 1.25
    }
  }
  cat('support:', supp, '\n')
  #length(doc)*supp>2
  itemsets <- eclat(doc, parameter = list(supp = supp, maxlen = 3))
  r <- ruleInduction(itemsets, confidence = .5)
}

rules.to.graph<-function(x,topN=70){
  x <- sort(x, by = "lift")
  topN<- min(topN,length(x))
  x<- x[1:topN]
  x<-as(x,'data.frame')
  x$rules<-gsub('\\{|\\}| ','',x$rules)
  tmp<-sapply(x$rules,function(a){unlist(strsplit(a,'=>'))} ,USE.NAMES = F)
  tmp<-t(tmp)
  x<-cbind(x,f1=tmp[,1],t1=tmp[,2])
  x$ruleid<-paste0('r',1:nrow(x))
  tmp1=t(apply(x[,c('f1','ruleid')],1,function(a){a1=unlist(strsplit(a[1],','));paste0(a1,'__',a[2])}))
  tmp2=t(apply(x[,c('ruleid','t1')],1,function(a){a2=unlist(strsplit(a[2],','));paste0(a[1],'__',a2)}))
  tmp3<- c(unlist(tmp1),tmp2)
  edges<-t(sapply(tmp3,function(a){unlist(strsplit(a,'__') )},USE.NAMES = F))
  edges<-as.data.frame(edges,stringsAsFactors = F)
  colnames(edges)<- c('from','to') 
  v<- sort(unique(c(edges$from,edges$to)))
  vertices<- data.frame(label=v,lift=1,stringsAsFactors = F)
  
  rule_label<- vertices$label[grep('r[0-9]',vertices$label)]
  vertices$lift[grep('r[0-9]',vertices$label)]<- x$lift[match(rule_label,x$ruleid)]

  vertices$group<-0
  vertices$group[vertices$label %in% pos_words] = 1
  vertices$group[vertices$label %in% neg_words] = -1
  vertices$group[grep('r[0-9]',vertices$label)]<- 2
  
  center<-unique(x$t1)
  return(list(vertices=vertices,edges=edges,center=center))
}

###########################################################################################
generate_one_results = function(plt_line=c('小牛','小牛')) {
  plt<- plt_line[1]
  plt_pattern<- plt_line[2]
  
  rule_pattern=unlist(strsplit(plt_pattern,'/|\\|'))
  plt_pattern=plt
  
  if(plt=='ALL_XIAONIU'){
    plt='小牛'
    plt_pattern=paste0(rule_pattern,collapse = '|')
    
  }
  
  doc<-txt[grep(plt_pattern,txt$plt_name),]
  cat(plt,'全部',nrow(doc),'.....\n')
  if(length(doc$plt_mult_name)>0){
    doc1 <- remove_words(doc$content_words_keep)
    r <- produce_rules(doc1)
    rule_pattern1 <- rule_pattern[rule_pattern %in% itemLabels(r)]
    sort_rules <- subset(r, subset = (rhs %in% rule_pattern1))
    js <- rules.to.graph(sort_rules)
    json_all = toJSON(js)
  }
  else{
    json_all=''
  }
  out_all <- c( plt,'全部' ,as.character(json_all),nrow(doc))
  
  
  doc_neg<- doc$content_words_keep[doc$sentiment == '敏感']
  cat(plt,'敏感',length(doc_neg),'.....\n')
  if(length(doc_neg)>0){
    doc_neg <- remove_words_neg(doc_neg)
    r <- produce_rules(doc_neg)
    rule_pattern2 <- rule_pattern[rule_pattern %in% itemLabels(r)]
    sort_rules <- subset(r, subset = (rhs %in% rule_pattern2))
    js <- rules.to.graph(sort_rules)
    json_neg = toJSON(js)
    
  }
  else{
    json_neg = ''
  }
  out_neg <- c( plt,'敏感' ,as.character(json_all),length(doc_neg))
  
  
  doc_pos <- doc$content_words_keep[doc$sentiment == '积极']
  cat(plt,'积极',length(doc_pos),'.....\n')
  if(length(doc_pos)>0){
    doc_pos<-remove_words_pos(doc_pos)
    r <- produce_rules(doc_pos)
    rule_pattern3 <- rule_pattern[rule_pattern %in% itemLabels(r)]
    sort_rules <- subset(r, subset = (rhs %in% rule_pattern3))
    js <- rules.to.graph(sort_rules)
    json_pos = toJSON(js)
  }
  else{
    json_pos=''
  }
  out_pos <- c( plt,'积极' ,as.character(json_all),length(doc_pos))
  
  out<-rbind(out_all,out_neg,out_pos)
  row.names(out)<-NULL
  rm( doc, r, sort_rules, js,doc_neg,doc_pos,out_all,out_pos,out_neg)
  rm(json_all,json_neg,json_pos)
  gc(verbose = T, reset = T)
  
  return(out)
}

n=nrow(plt_list)
## 注册CPU个数
cl <- makePSOCKcluster(6)
registerDoParallel(cl)
clusterExport(cl = cl, c('produce_rules','rules.to.graph','remove_words','remove_words_neg',
                         'remove_words_pos','sent_words','neg_words','pos_words'))
t1=Sys.time()
out<-foreach(m=1:n,.combine = rbind,.packages = c('arules','jsonlite'),.verbose = T,.errorhandling='remove') %dopar%  
  generate_one_results(plt_line=c(plt_list$plt_name[m],plt_list$pattern[m]))
   
cat(Sys.time()-t1)
stopCluster(cl)

out = as.data.frame(out,stringsAsFactors = F)
colnames(out) <- c('plt_name','sentiment','rule_json','article_num')
out$article_num<-as.integer(out$article_num)
out<-out[out$article_num>0,]

write.table(out,file = 'js_ciyun.txt',sep = '\u0001',row.names = F,col.names = F)
