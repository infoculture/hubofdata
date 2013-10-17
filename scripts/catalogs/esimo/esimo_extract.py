#!/usr/bin/env python
# coding: utf8
"""
EMISS data extractor
"""
import csv
import json
import os, os.path
from urllib import urlopen, unquote_plus, urlencode
import urllib2
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

import mechanize
import cookielib
import time
BASE_URL = 'http://portal.esimo.ru'

CATALOG_DATA_URL = "http://portal.esimo.ru/portal/portal/esimo-user/data/SRBDPortalPortletWindow?set=5&action=b&cacheability=PAGE&_=1375772560123"
CATALOG_FILE = 'data/catalog.json'
PAGE_URLPAT = 'http://portal.esimo.ru/dataview/viewresource?resourceId=%s'
CODES_URL = 'http://portal.esimo.ru/dataview/getcodes'
RES_URL = 'http://portal.esimo.ru/dataview/getresourcetable'
PAGE_LEN = 500
DATA_LIMIT = 350000

class DataExtractor:
    """Data extractor from ESIMO"""

    def __init__(self):
        pass

    def extract_catalog(self):
        u = urlopen(CATALOG_DATA_URL)
        data = u.read()
        u.close()
        f = open(CATALOG_FILE, 'w')
        parsed = json.loads(data)
        f.write(json.dumps(parsed, indent=4, sort_keys=True))
        f.close()

    def __get_codes(self, rid):
        values = {'resourceId' : rid}
        data = urlencode(values)
        req = urllib2.Request(CODES_URL, data)
        response = urllib2.urlopen(req)
        return response.read()

    def __extract_table_data(self, rid, start, total):
        headers = { 'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                    'Host' :'portal.esimo.ru',
                    'X-Requested-With' : 'XMLHttpRequest',
                    'Content-Type' : 'application/x-www-form-urlencoded',
                    'Origin' : 'http://portal.esimo.ru',
                    'Referer' : 'http://portal.esimo.ru/dataview/viewresource?resourceId=%s' % rid,
                    }
        values = {'resourceId' : rid,
                  'sEcho' : '1',
                  'filter' : '',
                  'distinct' : '',
                  'iDisplayLength': str(total),
                  'iDisplayStart': str(start),}
        data = urlencode(values)
        req = urllib2.Request(RES_URL, data, headers)
        response = urllib2.urlopen(req)
        jsdata = json.loads(response.read())
        aaData = jsdata['aaData']
        return aaData

    def __get_table(self, rid):
        total = self.__get_table_len(rid)
        print 'Table lengh', total
        if total > DATA_LIMIT:
            print 'Table is too big!'
            return total, []
        pages = total / 100
        if total % 100 > 0:
            pages += 1
        data = []
        for i in range(0, pages, 1):
            data.extend(self.__extract_table_data(rid, i*PAGE_LEN, PAGE_LEN))
        print 'Extracted data', len(data)
        return total, data

    def __get_table_len(self, rid):
        headers = { 'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                    'Host' :'portal.esimo.ru',
                    'X-Requested-With' : 'XMLHttpRequest',
                    'Content-Type' : 'application/x-www-form-urlencoded',
                    'Origin' : 'http://portal.esimo.ru',
                    'Referer' : 'http://portal.esimo.ru/dataview/viewresource?resourceId=%s' % rid,
                    }
        values = {'resourceId' : rid,
                  'sEcho' : '1',
                  'filter' : '',
                  'distinct' : '',
                  'iDisplayLength': '1',
                  'iDisplayStart': '0',}
        data = urlencode(values)
        req = urllib2.Request(RES_URL, data, headers)
        response = urllib2.urlopen(req)
        jsdata = json.loads(response.read())
        totalnum = int(jsdata['iTotalRecords'])
        return totalnum

    def store_table(self, rid):
        codes = self.__get_codes(rid)
        f = open('data/raw/codes_%s.json' % rid, 'w')
        f.write(codes)
        f.close()
        total, data = self.__get_table(rid)
        jsdata = json.loads(codes)
        keys = []
        if len(data) == 0:
            return total
        for r in jsdata:
            keys.append(r['nameshort'])
        f = open('data/raw/data_%s.csv' % rid, 'w')
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for r in data:
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)
        f.close()
        return total

    def extract_all_raw(self):
        f = open(CATALOG_FILE, 'r')
        data = f.read()
        f.close()
        parsed = json.loads(data)
        i = 0
        keys = ['id', 'total']
        f = open('data/processed.csv', 'w')
        s = ('\t'.join(keys)).encode('utf8') + '\n'
        f.write(s)
        for item in parsed['aaData']:
            if item['accessConstraints'] == 'unclassified':
                if item['datasourceStorageType'] in ['DATA_FILE', 'DBMS']:
                    filename = 'data/raw/data_%s.csv' % item['resourceId']
                    if os.path.exists(filename):
                        print 'Already processed', item['resourceId']
                        continue
                    else:
                        print item['datasourceStorageType'], PAGE_URLPAT % item['resourceId']
                    thelen = self.store_table(item['resourceId'])
                    s = ('\t'.join([item['resourceId'], str(thelen)])).encode('utf8') + '\n'
                    f.write(s)
        f.close()
        print i


if __name__ == "__main__":
    ext = DataExtractor()
#    ext.extract_catalog()
    ext.extract_all_raw()


