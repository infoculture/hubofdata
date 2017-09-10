#!/usr/bin/env python
# coding: utf8
"""
Websites
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
BASE_URL = 'http://government.ru'
LIST_URL = 'http://government.ru/ministries/'

class DataExtractor:
    """Data extractor from government websites"""

    def __init__(self):
        pass

    def extract_govlist(self):
        data = urllib2.urlopen(LIST_URL).read()
        soup = BeautifulSoup(data)
        ul = soup.find('ul', attrs={'class' : 'departments col col__wide'})
        lis = ul.findAll('a')
        f = open(CATALOG_FILE, 'w')
        keys = ['name', 'url']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for item in lis:
            url = BASE_URL + item['href']
            name = item.string
            data = urllib2.urlopen(url).read()
            ps = BeautifulSoup(data)
            weburl = ""
            for p in ps.findAll('p', attrs={'class' : 'vcard_data'}):
                alink = p.find('a')
                if alink is not None and alink['href'][:4] == 'http':
                    weburl = alink['href'].rstrip('/')
                    break
            r = [name, weburl]
            print name, weburl
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)
        f.close()




    def extract_all_raw(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        i = 0
        f = open(PROCESSED_FILE, 'w')
        keys = ['name', 'url', 'title', 'feedurl', 'feedtype']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        ext = FeedsExtractor()
        for ritem in reader:
            print 'Processing', ritem['name']
            result = ext.find_feeds_deep(ritem['url'], True)
            if result.has_key('items') and len(result['items']) > 0:
                uniq = {}
                for item in result['items']:
                    if item['url'] not in uniq.keys():
                        uniq[item['url']] = item
                    elif uniq[item['url']].has_key('title') and item.has_key('title'):
                        uniq[item['url']]['title'] = item['title']
                for k, item in uniq.items():
                    print '-', item['url'], item['title'] if item.has_key('title') else ""
                    r = [ritem['name'].decode('utf8'), ritem['url'].decode('utf8'), item['title'] if item.has_key('title') else "", item['url'], item['feedtype']]
                    s = ('\t'.join(r)).encode('utf8') + '\n'
                    f.write(s)
                    print item
        f.close()




if __name__ == "__main__":
    ext = DataExtractor()
 #   ext.extract_govlist()
    ext.extract_all_raw()


