#!/usr/bin/env python
# coding: utf8
"""
Feeds of FAS RF
"""
import csv
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

import mechanize
import cookielib
import time
from persimmon.tools.findfeeds import FeedsExtractor

CATALOG_FILE = 'data/websites.csv'
PROCESSED_FILE = 'data/processed_websites.csv'
BASE_URL = 'http://mvd.ru'
LIST_URL = 'http://fas.gov.ru/territorial-authorities/'
FEED_PATTERN = '%s/rss_feed.xml'

class DataExtractor:
    """Data extractor from government websites"""

    def __init__(self):
        pass

    def extract_list(self):
        data = urllib2.urlopen(LIST_URL).read()
        soup = BeautifulSoup(data)
        lis = soup.findAll('dt', attrs={'class' : 'name'})
        f = open(CATALOG_FILE, 'w')
        keys = ['name', 'url']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for item in lis:
            u = item.find('a')
            name = u.string
            weburl = FEED_PATTERN % u['href'].rstrip('/')
            r = [name, weburl]
            print name, weburl
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)
        f.close()




if __name__ == "__main__":
    ext = DataExtractor()
    ext.extract_list()


