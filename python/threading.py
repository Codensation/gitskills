# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 17:18:23 2018

@author: xn088969
"""

import threading
import time
from queue import Queue
#def thread_job():
#    print('T1 start\n')
#    for i in range(10):
#        time.sleep(0.1)
#        print('T1 finish\n')
#        
#def T2_job():
#    print('T2 start\n')
#    print('T2 finish\n')
#def main():
#    added_thread = threading.Thread(target=thread_job,name='T1')
#    thread2=threading.Thread(target=T2_job,name='T2')
#    added_thread.start()
#    thread2.start()
#    added_thread.join()
#    thread2.join()
#    print('all done\n')
    
    
#    print(threading.active_count())
#    print(threading.enumerate())
#    print(threading.current_thread())

def job(l,q):
    for i in range(len(l)):
        l[i]=l[i]**2
    q.put(l)
def multithreading():
    q=Queue()
    threads=[]
    data=[[1,2,3],[3,4,5],[4,4,4],[5,5,5]]
    for i in range(4):
        t=threading.Thread(target=job,args=(data[i],q))
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()
    results=[]
    for _ in range(4):
        results.append(q.get())
    print(results)


if __name__== "__main__":
    multithreading()
    
    
SELECT c.userid,
ceil(datediff('2018-08-22',c.mind)/30)
FROM(
    SELECT b.userid,
    b.date_tm,
    MAX(b.date_tm) OVER(PARTITION BY b.userid) AS maxd,
    MIN(b.date_tm) OVER(PARTITION BY b.userid) AS mind
    FROM (SELECT userid,to_date(clickdate) AS date_tm
          FROM dwd_data.dwd_zx_log_tj_click
          WHERE part_dt='2018-08-22' AND userid is not null
          ) AS b
    GROUP BY b.userid,b.date_tm) AS c
GROUP BY c.userid
limit 20;
    

sql="""
select userid,origuserid,to_date(clickdate) date
	from dwd_data.dwd_zx_log_tj_click 
	where part_dt='2018-08-30'AND userid='07a159d2db90e36fda87bc4620'
	group by userid,origuserid,to_date(clickdate)
"""
c=spark.sql(sql).rdd.collect()

sql="""
select userid,origuserid,orig_usr_id
    from dwd_data.dwd_zx_log_tj_click
    where part_dt='2018-08-30' AND userid='07a159d5d29de86cdc88bc4620'
"""
c=spark.sql(sql).rdd.collect()