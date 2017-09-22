#!/usr/bin/python3
import psycopg2
from datetime import date


def connect():
    try:
        return psycopg2.connect("dbname=news")
    except:
        print("Connection failed")

def formatted_print(query_results, header, prefix):

    print('\n'+header)
    for i in range(len(query_results)):
        print(prefix.format(query_results[i][0], query_results[i][1]))
    return cur.fetchall()

db = connect()        
cur = db.cursor()
cur.execute("""select title ,
(select count(log.id)
from log
where log.path = concat('/article/',articles.slug))as views
from articles
order by views desc
limit 3;""")
# this query uses subquery to count the unique logids viewing the article.

res = cur.fetchall()
formatted_print(res, "Three most popular articles of all time:"
                , "{} - {} views")

cur.execute("""select name,
sum((select count(log.id)
from log
where log.path = concat('/article/',articles.slug))) as views
from articles,authors
where authors.id =author
group by author,name
order by views desc;""")
# this query add the logids visiting articles belonging to authors

res = cur.fetchall()
formatted_print(res, "The most popular authors of all time:"
                , "{} - {} views")

cur.execute("""select a.dt,round(cast(neg/tot*100 as numeric),2)
from (select DATE(time) as dt,cast(count(*) as float) as tot
from log
group by date(time)) as a,
(select Date(time) as dt,cast(count(*) as float) as neg
from log
where not status='200 OK'
group by Date(time)) as b
where a.dt = b.dt
and neg/tot*100.0 > 1;""")
# this query uses two subquery in from clause to join table containing error
# status(neg) and total number request for eaxh date(tot) and performs devision
# to select dates on which the error is more than 1%.

res = cur.fetchall()
print("\nThe days on which more 1% request leads to error:")
for i in range(len(res)):
    dt = res[i][0].strftime("%A %d, %B %Y")
    print("{} - {}% erros".format(dt, res[i][1]))    
db.close()
