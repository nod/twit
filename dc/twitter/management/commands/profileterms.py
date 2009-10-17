from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "<min_count> <path to file of terms> prints out terms with at least min_count unique terms"

    def handle(self, *fixture_labels, **options):
        from twitter.models import Tweet, TwitAccount
        min_count = int(fixture_labels[0])
        filepath = fixture_labels[1]
        userdict = {}
        for term in open(filepath):
            term = term.strip()
            if not term: continue
            tweets = Tweet.objects.filter(text__icontains=term)
            for t in tweets:
                sname = t.owner.screen_name.lower().strip()
                if not userdict.has_key(sname):
                    userdict[sname] = set() 
                userdict[sname].add(term)
        for u,t in userdict.iteritems():
            if len(t) >= min_count:
                print u, ', '.join(t)

