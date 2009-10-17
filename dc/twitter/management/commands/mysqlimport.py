from django.core.management.base import BaseCommand

from django.utils.encoding import smart_unicode, force_unicode
import sys
import urllib2


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
    help = "<dir to import> crawls a given directory of xml files and creates TwitterUsers"

    def handle(self, *fixture_labels, **options):
        from datetime import datetime
        from twitter.models import HashTag, Tweet, TwitAccount
        import MySQLdb
        import re
        nickre = re.compile('@([\w_\d]+)')
        errors = 0
        counter = 8956003  #  8374002
        flag = 'twit01convert'
        update_flag(flag, 1)

        flagstatus = 0
        while 1:
            update_stat(flag, counter)
            if flagstatus:
                flagstatus = 1
                update_flag(flag, 2, "counter=%d %s" % (counter, str(datetime.datetime.now()) ) )
            else:
                flagstatus = 0
                update_flag(flag, 3, "counter=%d %s" % (counter, str(datetime.now()) ) )
            mydb = MySQLdb.connect(user="trashjk", passwd="trashjk", db="trashjk")
            mycur = mydb.cursor(MySQLdb.cursors.DictCursor) # yes, i'm recreating cursor every so often
            num = mycur.execute('select * from status limit 10000 offset %d' % counter)
            if num == 0: break
            smallcounter = 0
            for status in mycur:
                status['screenName'] = status['screenName'].lower()
                try:
                    counter += 1
                    smallcounter += 1
                    ta,new = TwitAccount.objects.get_or_create(screen_name=status['screenName'], uid=-1)
                    if new: ta.save()
                    t,_new = Tweet.objects.get_or_create(
                            tid=status['statusid'],
                            owner=ta,
                            text=unicode(status['text'], errors='ignore'),
                            timestamp=status['created_at'],
                            )
                    if not _new: continue
                    t.save()
                    for nick in nickre.findall(status['text']):
                        repacct,new = TwitAccount.objects.get_or_create(screen_name=nick.lower(), uid=-1)
                        if new:
                            repacct.save()
                        t.to_users.add(repacct)
                    t.save()
                    if smallcounter > 2000:
                        smallcounter = 0
                        print "loaded:%d  errors:%d" % (counter, errors)
                        update_stat("twit01convert", counter)
                    del t
                    del ta
                except:
                    errors += 1
                    print "Some exception..."
                    update_stat("tw01err", errors)
            mydb.commit()
            # i want to force it to garbage collect this
            mycur.close()
            del mycur
        update_flag(flag, 0, "counter=%d %s" % (counter, str(datetime.datetime.now()) ) )

        print "\n-----------------------------\nSummary"
        print "tweets imported:",counter
