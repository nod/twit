from django.core.management.base import BaseCommand

from django.utils.encoding import smart_unicode, force_unicode
import sys
import urllib2

class Command(BaseCommand):
    help = "<dir to import> crawls a given directory of xml files and creates TwitterUsers"

    def handle(self, *fixture_labels, **options):
        from datetime import datetime
        from glob import glob
        from twitter.models import HashTag, Tweet, TwitAccount
        from BeautifulSoup import BeautifulSoup as BS
        from dateutil.parser import parse as parse_date
        import re
        nickre = re.compile('@<a.+?>([\w_\d]+)<')
        errors = counter = 0
        for d in fixture_labels:
            print "preparing to work on", d
            for f in glob("%s/*.xml" % d):
                soup = BS(open(f))
                for status in soup.findAll('status'):
                    try:
                        uid = status.user.id.contents[0]
                        ta,new = TwitAccount.objects.get_or_create(uid=uid)
                        if new:
                            ta.screen_name = status.user.screen_name.contents[0].lower()
                            ta.save()
                        # XXX mysql is completely lame.  it can't handle datetime 
                        # with tz info so we have to kludge it and create a datetime
                        # object w/o tz... i really hate mysql at times like this.-jk
                        ts = list(parse_date(status.created_at.contents[0]).utctimetuple()[:7])
                        ts = datetime(*ts)
                        t = Tweet(
                                tid=status.id.contents[0],
                                owner=ta,
                                text=status.text.contents[0][0:255],
                                timestamp=ts,
                                )
                        t.save()
                        for nick in nickre.findall(str(status.text.contents[0])):
                            repacct,new = TwitAccount.objects.get_or_create(screen_name=nick.lower())
                            if new:
                                repacct.uid = -1
                                repacct.save()
                            t.to_users.add(repacct)    
                        counter += 1
                        if not counter % 20:
                            print ".",
                        if not counter % 300:
                            print "loaded:%d  errors:%d" % (counter, errors)
                            u = "http://infolab.tamu.edu/stats/new/%s/%d" % (
                                "twit01import", counter )
                            _ = urllib2.urlopen(u).read()
                            _.close()
                            u = "http://infolab.tamu.edu/stats/new/%s/%d" % (
                                "twit01error", errors )
                            _ = urllib2.urlopen(u).read()
                            _.close()
                    except:
                        errors += 1
                        # print "ERR: text=", status.text.contents[0]
                        # sys.exit(1)
                        print "Some exception...", status.prettify()
                        # break

        print "\n-----------------------------\nSummary"
        print "tweets imported:",counter,"\nerrors encountered:",errors
