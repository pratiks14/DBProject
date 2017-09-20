import psycopg2
import re
from datetime import date

db = psycopg2.connect(database='news'	)
cur=db.cursor()
cur.execute("select title ,(select count(log.id) from log where log.path = concat('/article/',articles.slug))as views from articles order by views desc limit 3;")
res=cur.fetchall()
print("Three most popular articles of all time:")
for i in range(len(res)):
	print("{} - {} views".format(res[i][0],res[i][1]))

cur.execute("select name,sum((select count(log.id) from log where log.path = concat('/article/',articles.slug))) as views from articles,authors where authors.id =author group by author,name order by views desc;")
res=cur.fetchall()
print("\nThe most popular authors of all time:")
for i in range(len(res)):
	print("{} - {} views".format(res[i][0],res[i][1]))	

cur.execute("select a.dt,round(cast(neg/tot*100 as numeric),2) from (select DATE(time) as dt,cast(count(*) as float) as tot from log group by date(time)) as a,(select Date(time) as dt,cast(count(*) as float) as neg from log where not status='200 OK' group by Date(time))as b where a.dt = b.dt and neg/tot*100.0 > 1;  ")
res=cur.fetchall()
print("\nThe days on which more 1% request leads to error:")
for i in range(len(res)):
	dt=res[i][0].strftime("%A %d, %B %Y")
	print("{} - {}% erros".format(dt,res[i][1]))

db.close()