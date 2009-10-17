from django.core.management.base import BaseCommand

from django.utils.encoding import smart_unicode, force_unicode
import sys
import urllib2


class Command(BaseCommand):
    help = "<dir to import> crawls a given directory of xml files and creates TwitterUsers"

    def handle(self, *fixture_labels, **options):
        from datetime import datetime
        from twitter.models import HashTag, Tweet, TwitAccount


