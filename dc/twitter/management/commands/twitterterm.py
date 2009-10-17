from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "<term> retrieves tweets with that term"

    def handle(self, *fixture_labels, **options):
        from twitter.models import Tweet, TwitAccount
        term = fixture_labels[0].strip()
        if not term: return
        tweets = Tweet.objects.filter(text__icontains=term)
        for t in tweets:
            print t.owner.screen_name, t.timestamp, t.text



