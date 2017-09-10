#!/usr/bin/env python
# coding: utf8
"""
Kremlin letters data extractor
"""
import csv
import json
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
import urllib
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup
import time

import mechanize
import cookielib
import time
from urlparse import urljoin
from StringIO import StringIO

BASE_URL = 'http://kremlin.ru'
FILE_PATH = 'data/letters'
RAW_PATH = 'data/letters/raw'
CATALOG_FILE = 'data/letters/letters.csv'
LIST_PATTERNS = [
    {'name' : 'greeting', 'url' : 'http://kremlin.ru/letters/greeting', 'top_page': 28},
    {'name' : 'condolences', 'url' : 'http://kremlin.ru/letters/condolences', 'top_page': 10},
    {'name' : 'greetings', 'url' : 'http://kremlin.ru/letters/greetings', 'top_page': 119},
]

class DataExtractor:
    """Data extractor for EGE"""

    def __init__(self):
        pass

    def process_pat(self, pat):
        filename = os.path.join(FILE_PATH, pat['name'] + '.csv')
        f = open(filename, 'w')
        keys = ['title', 'url', 'id']
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for page in range(1, pat['top_page'] + 1, 1):
            url = '%s?page=%d' % (pat['url'], page)
            print 'Processing', url
            u = urllib2.urlopen(url)
            data = u.read()
            soup = BeautifulSoup(data)
            records = soup.findAll('div', attrs={'class' : 'hentry'})
            for record in records:
                rurl = record.find('h4').find('a')
                r = [rurl.string.strip(), urljoin(BASE_URL, rurl['href'])]
                s = ('\t'.join(r)).encode('utf8') + '\n'
                f.write(s)
                print rurl['href']
        print "Finished", pat['name']
        f.close()


    def extract_all_links(self):

        for pat in LIST_PATTERNS:
            self.process_pat(pat)

    def extract_all_raw(self):
        for pat in LIST_PATTERNS:
            reader = csv.DictReader(open(os.path.join(FILE_PATH, pat['name'] + '.csv'), 'r'), delimiter="\t")
            for r in reader:
                id = r['url'].rsplit('/', 1)[-1]
                raw_file = '%s.json' % id
                fullpath = os.path.join(RAW_PATH, raw_file)
                if os.path.exists(fullpath):
                    print 'Skipping', id
                    continue
                u = urllib2.urlopen(r['url'])
                data = u.read()
                soup = BeautifulSoup(data)
                d = soup.find('div', attrs={'id' : 'letter_content'})
                title = d.find('h5').string
                mark = d.find('p', attrs={'class' : 'tt-date'})
                thedate = mark.string
                tfrom = d.find('p', attrs={'class' : 'tt-from'}).string
                io = StringIO()
                current = mark
                while True:
                    current = current.findNextSibling(text=None)
                    if not current or (current.has_key('class') and current['class'] == 'tt-from'): break
                    io.write(str(current))
#                text = io.getvalue()
                f = open(fullpath, 'w')
                item = {'id' : id, 'category' : pat['name'],  'title' : title, 'thedate' : thedate, 'from' : tfrom, 'text' : io.getvalue()}
                f.write(json.dumps(item, indent=4))
                print 'Processed', id
                f.close()
                time.sleep(1)

    def merge_raw(self):
        for pat in LIST_PATTERNS:
            reader = csv.DictReader(open(os.path.join(FILE_PATH, pat['name'] + '.csv'), 'r'), delimiter="\t")
            records = []
            print 'Merging', pat['name']
            for r in reader:
                id = r['url'].rsplit('/', 1)[-1]
                raw_file = '%s.json' % id
                fullpath = os.path.join(RAW_PATH, raw_file)
                if os.path.exists(fullpath):
                    f = open(fullpath, 'r')
                    record = json.loads(f.read())
                    records.append(record)
                    f.close()
            results = {'total' : len(records), 'items' : records}
            f = open(os.path.join(FILE_PATH, pat['name'] + '_data.json'), 'w')
            f.write(json.dumps(results, indent=4))
            f.close()




if __name__ == "__main__":
    ext = DataExtractor()
#    ext.extract_all_links()
#    ext.extract_all_raw()
    ext.merge_raw()


