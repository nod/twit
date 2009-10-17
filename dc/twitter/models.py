from django.db import models
from django.conf import settings

# we need a bigint type, so make one
class BigIntegerField(models.IntegerField):
    empty_strings_allowed=False
    def get_internal_type(self):
        return "BigIntegerField"
    def db_type(self):
        return 'NUMBER(19)' if settings.DATABASE_ENGINE == 'oracle' else 'bigint'


# ################################### Our Models ######################

class TwitAccount(models.Model):
    uid = BigIntegerField()
    screen_name = models.CharField(max_length=64)
    name = models.CharField(max_length=120)


class HashTag(models.Model):
    text = models.CharField(max_length=32)


class Tweet(models.Model):
    tid = BigIntegerField()
    owner = models.ForeignKey(TwitAccount, related_name='owner')
    timestamp = models.DateTimeField()
    text = models.CharField(max_length=255)
    hashtags = models.ManyToManyField(HashTag, blank=True, null=True,
        related_name='hashtags')
    to_users = models.ManyToManyField(TwitAccount, blank=True, null=True,
        related_name='to_users') 


class DecayedScore(models.Model):
    tweet = models.ForeignKey(Tweet)
    as_of_time = models.DateTimeField()
    score = models.FloatField()
    decay = models.CharField(max_length=8) # 3m would be 3 mins
    from_user = models.ForeignKey(TwitAccount, related_name='from_user')
    to_user = models.ForeignKey(TwitAccount, related_name='to_user')


class RawDecayedScore(models.Model):
    tweet = models.ForeignKey(Tweet)
    as_of_time = models.DateTimeField()
    tweet_time = models.DateTimeField()
    score = models.FloatField()
    decay = models.CharField(max_length=8) # 3m would be 3 mins
    from_user = models.ForeignKey(TwitAccount, related_name='rawfrom_user')
    to_user = models.ForeignKey(TwitAccount, related_name='rawto_user')
