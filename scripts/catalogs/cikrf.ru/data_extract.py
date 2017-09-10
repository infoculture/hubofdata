#!/usr/bin/env python
# coding: utf8
"""
Data extractor (cikrf.ru) party reports
"""
import csv
import json
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
import urllib
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup
from urlparse import urljoin
import mechanize
import cookielib
import time
BASE_URL = 'http://cikrf.ru'

CATALOG_FILE = 'data/datasets.csv'
YEARS = ['2007', '2008', '2009', '2010', '2011', '2012', '2013']
LIST_PATTERN = 'http://www.cikrf.ru/politparty/finance/%s/'


class DataExtractor:
    """Data extractor for EGE"""

    def __init__(self):
        pass

    def extract_catalog(self):
        f = open(CATALOG_FILE, 'w')
        keys = ['year', 'category_name', 'category_url', 'name', 'url']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for year in YEARS:
            listurl = LIST_PATTERN % year
            print listurl
            u = urllib2.urlopen(listurl)
            data = u.read()
            u.close()
            soup = BeautifulSoup(data)
            lis = soup.findAll('a', attrs={'class' : 'listitem'})
            catname = u'Сведения о поступлении и расходовании средств политических партий за %s год' % year
            for ufile in lis:
                name = ufile.string
                weburl = urljoin(BASE_URL, ufile['href'])
                r = [year, catname, listurl, name, weburl]
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
    ext.extract_catalog()
    ext.extract_all_raw()


