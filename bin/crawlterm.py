#!/usr/bin/env python

import re
from time import time
from glob import glob
from os.path import basename
from BeautifulSoup import BeautifulSoup as BS

SRC_DIR = "/project/wdim/crawlData/Full_Profiles/"
DST_DIR = "/project/wdim/dc/data/fromto/"


def main():
    hashtagre = re.compile('#(iran[\w_\d]+)')
    statusre = re.compile('status')
    tsre = re.compile('title="(.+?)"')
    for f in glob("%s/*.html" % SRC_DIR):
        from_nick = basename(f).split('.')[0] # assumes blah.html
        print "working on", from_nick
        fp = open(f)
        bs = BS(fp.read())
        fp.close()
        outf = open(
            '%s%s/%s.fromto' %(DST_DIR,from_nick[0].lower(),from_nick),
            'w' )
        for st in bs.findAll('tr', attrs={'class':statusre}):
            text = st.find('span', attrs={'class':'entry-content'})
            hashtags = hashtagre.findall(str(text))
            if not hashtags: continue  # no @hashtags
            spants = st.find('span', attrs={'class':'published'})
            ts = tsre.findall(str(spants))[0]
            print >>outf, ts, ' '.join(atreplies)
        outf.close() 


if __name__ == '__main__':
    print "starting:", time()
    main() 
    print "ending:", time()

