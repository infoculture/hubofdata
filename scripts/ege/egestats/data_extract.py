#!/usr/bin/env python
# coding: utf8
"""
EGE.EDU.RU data extractor
"""
import csv
import json
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
import urllib
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

import mechanize
import cookielib
import time
BASE_URL = 'http://ege.edu.ru'

CATALOG_FILE = 'data/datasets.csv'
LIST_URL = 'http://ege.edu.ru/ru/main/satistics-ege/'

class DataExtractor:
    """Data extractor for EGE"""

    def __init__(self):
        pass

    def extract_catalog(self):
        u = urllib2.urlopen(LIST_URL)
        data = u.read()
        u.close()
        soup = BeautifulStoneSoup(data)
        lis = soup.find('div', attrs={'class' : 'content'}).findAll('li')
        f = open(CATALOG_FILE, 'w')
        keys = ['name', 'url']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for item in lis:
            u = item.find('a')
            name = u.string
            weburl = u['href']
            r = [name, BASE_URL + weburl]
            print name, weburl
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)
        f.close()


    def extract_all_raw(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        for item in reader:
            url = item['url']
            filename = url.rsplit('/')[-1]
            filepath = 'data/raw/'
            urllib.urlretrieve(url, filepath + filename)
            print 'Downloaded', url


if __name__ == "__main__":
    ext = DataExtractor()
#    ext.extract_catalog()
    ext.extract_all_raw()


