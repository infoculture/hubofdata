#!/usr/bin/env python
# coding: utf8
"""
Data extractor (minfin.ru)
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
BASE_URL = 'http://www1.minfin.ru'

CATALOG_FILE = 'data/datasets.csv'
LIST_URL_PAT = ['http://www1.minfin.ru/ru/budget/federal_budget/budj_rosp/index.php?pg4=%s']
LIST_PAGES = ['1', '2', '3', '4']

class DataExtractor:
    """Data extractor for Minfin Budget"""

    def __init__(self):
        pass

    def extract_catalog(self):
        f = open(CATALOG_FILE, 'w')
        keys = ['category_name', 'category_url', 'name', 'url']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for listurl in LIST_URLS:
            print listurl
            u = urllib2.urlopen(listurl)
            data = u.read()
            u.close()
            soup = BeautifulSoup(data)
            lis = soup.findAll('div', attrs={'class' : 'submitted left'})
            for item in lis:
                item = item.parent
                u = item.find('a')
                category_name = u.string
                caturl = urljoin(BASE_URL, u['href'])
                print category_name, caturl

                ud = urllib2.urlopen(caturl)
                data = ud.read()
                ud.close()
                itemsoup = BeautifulSoup(data)
                files = itemsoup.findAll('div', attrs={'class' : 'file-attr'})
                for fname in files:
                    print fname
                    ufile = fname.find('a')
                    name = ufile.string
                    weburl = urljoin(BASE_URL, ufile['href'])
                    r = [category_name, caturl, name, weburl]
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


