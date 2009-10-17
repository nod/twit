#!/usr/bin/env python

import urllib2
import datetime
import MySQLdb
import re
from glob import glob
from os.path import basename
import beanstalkc

from math import e


def decay(lam, timepassed):
    return e ** (-1 * lam * timepassed)


def update_flag(flag, stat, txt=""):
   try:
        url = "http://infolab.tamu.edu/stats/newflag/%s/%d/%s/" % (flag, stat, urllib2.quote(txt))
        urllib2.urlopen(url).read()
   except:
        pass 


def update_stat(stat, val):
   try:
       u = "http://infolab.tamu.edu/stats/new/%s/%d" % ( stat, val )
       urllib2.urlopen(u).read()
   except: pass


def _db():
    return MySQLdb.connect(
        user="twit02",
        passwd="frayedwell",
        db="twit02",
        host="hamm.cs.tamu.edu" )


def _db_cur(db=None):
    if db is None: db = _db()
    return db.cursor(MySQLdb.cursors.DictCursor)


def parse_date(s):
    return datetime.datetime(*map(int, re.split('[^\d]', s)[:-1]))


def go(d, job_callback=None):
    flag = "fix50%s" % d
    update_flag(flag,1, ".. started") 
    update_stat(flag,0) 
    counter = smcntr = 0

    db = _db()
    cur = _db_cur(db)
    cur2 = _db_cur(db)
    cur3 = _db_cur(db)
    table_name = "edges_to_%s" % d.lower()
    cur.execute("select distinct to_user from " + table_name)

    dates = ["2008-12-%d" % x for x in range(1,32)]

    for row in cur:
        for dt in dates:
            date = datetime.datetime(*[int(x) for x in dt.split('-')])
            sql = "select to_user, from_user, max(ts) as ts from " + table_name + " where ts <= %s and to_user = %s group by to_user, from_user"
            cur2.execute(sql, (date, row['to_user'], )) 
            for scr in cur2:

                smcntr += 1
                if smcntr > 12000:
                    smcntr = 0
                    s = "inserted:%d work:%s" % (counter,scr['to_user'])
                    print s
                    update_flag(flag,2,s)
                    update_stat(flag,counter) 
                    if job_callback: job_callback() # reset the job timer

                tdelta = date - scr['ts']  
                timebetween = (tdelta.days * 24) + (tdelta.seconds / 60 / 60) # gets me hours
                scr03 = decay(.03, timebetween)
                if scr03 < .0001: continue
                scr10 = decay(.10, timebetween)
                scr50 = decay(.50, timebetween)
                sql = "insert into " + "scores_%s"%d + " values (%s, %s, %s, %s, %s, %s)"
                cur3.execute(sql, (scr['to_user'], scr['from_user'], date, scr03, scr10, scr50) )
                counter += 1

# create table scores_%s (to_user varchar(120), from_user varchar(120), as_of
# datetime, score03 float, score10 float, score50 float)

            db.commit()
    update_flag(flag,0,".. finished") 


if __name__ == '__main__':
    c = beanstalkc.Connection(host='localhost', port=11300)

    while 1:
        print "calling beanstalk.  *rrrring*"
        job = c.reserve(timeout=9) # wait 9s for a job from the beanstalk
        if not job:
            print "beanstalk has no more jobs"
            break
        print "talked to the beanstalk:", job
        go(job.body, lambda: job.touch())
        job.delete()


