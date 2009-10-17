#!/usr/bin/env python

import urllib2
import datetime
import MySQLdb
import re
from glob import glob
from os.path import basename
import beanstalkc


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


def _db_cur():
    return _db().cursor(MySQLdb.cursors.DictCursor)


def parse_date(s):
    return datetime.datetime(*map(int, re.split('[^\d]', s)[:-1]))


def crawldir(d, job_callback=None):
    flag = "twimp%s" % d
    update_flag(flag,1, ".. started") 
    update_stat(flag,0) 
    counter = smcntr = 0
    base_dir = "/project/wdim/dc/data/fromto"
    for f in glob("%s/%s/*.fromto" % (base_dir, d)):
        name = basename(f).replace(".fromto", "").lower()
        db = _db()
        c = db.cursor()
        for L in open(f):
            counter += 1
            smcntr += 1
            if smcntr > 8000:
                smcntr = 0
                s = "total:%d work:%s" % (counter,name)
                print s
                update_flag(flag,2,s)
                update_stat(flag,counter) 
                if job_callback: job_callback() # reset the job timer
            x  = L.split()               
            d = parse_date(x[0])
            for u in x[1:]:
                c.execute("""
                    insert into crawled_interactions 
                    (to_user, from_user, ts) values 
                    ( %s, %s, %s ) """,
                (name, u.lower(), d) )
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
        crawldir(job.body, lambda: job.touch())
        job.delete()


