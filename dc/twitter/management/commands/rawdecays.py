from django.core.management.base import BaseCommand

from django.utils.encoding import smart_unicode, force_unicode
import sys
import urllib2

from math import e

# XXX     score = e ** (RateOfDecay*UnitsOfTimePassed)

def decay(h_passed):
    RateOfDecay = 0.03
    return e ** (-1 * RateOfDecay * h_passed )


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


class Command(BaseCommand):
    help = """<date to decay>
will create decayed scores for all tweets prior to that date at midnight.  arg
needs to be handed in mm/dd/yyyy format"""

    def handle(self, *fixture_labels, **options):
        from datetime import datetime
        from twitter.models import HashTag, Tweet, TwitAccount, RawDecayedScore
        counter = 0

        from beanstalk import serverconn
        c = serverconn.ServerConn('localhost', 11300)

        j = c.reserve()

        print "talked to the beanstalk:", j
        if not j['data']: return

        cc,dd = j['data'].split() # should be formatted like "start_num mm/dd/yyyy"

        m,d,y = (int(i) for i in dd.strip().split("/"))
        from_time = datetime(y,m,d,0,0)
   
        smallcounter = 0
        counter = int(cc)

        flag = 'dec%d' % (j['jid'])

        update_stat(flag, counter)
        update_flag(flag, 1, "counter=%d %s"%(counter,str(datetime.now())))

        tweets = Tweet.objects.filter(
                    timestamp__lte=from_time ).order_by('timestamp')[counter:counter+30000]
        realcounter = 0
        for tweet in tweets:
            smallcounter += 1
            counter += 1
            if not smallcounter % 1000:
                update_flag(flag, counter, 'jobid=%d' % j['jid'])
            tdelta = from_time - tweet.timestamp
            timebetween = (tdelta.days * 24) + (tdelta.seconds / 60 / 60)
            s = decay(timebetween)
            if s < 0.00001:
                continue
            # print "decayed score=",s
            for u in tweet.to_users.all():
                RawDecayedScore(tweet=tweet,
                    as_of_time=from_time,
                    tweet_time=tweet.timestamp,
                    score=s,
                    decay=".03 h",
                    from_user=tweet.owner,
                    to_user=u
                    ).save()
                realcounter += 1
            if smallcounter > 5000:
                smallcounter = 0
                update_stat(flag, counter)
                update_flag(flag, 1)
        # XXX     score = e ** (RateOfDecay*UnitsOfTimePassed)
        update_flag(flag, 0, "added:%d %s" % (realcounter, j['data'] ) )
        c.delete(j['jid'])


