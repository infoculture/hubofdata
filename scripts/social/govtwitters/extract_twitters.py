#!/usr/bin/env python
# coding: utf8
"""
Twitters
"""
import csv
import os, os.path


CATALOG_FILE = 'twitters.csv'

class DataExtractor:
    """Data extractor from government twitters"""

    def __init__(self):
        pass


    def extract_all_raw(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        i = 0
        for item in reader:
            print 'Processing', item['twittername']
            if os.path.exists('data/%s.csv' %(item['twittername'])): continue
            os.system('twitter-archiver -o -s data %s' % (item['twittername']))
            os.rename('data/%s' % item['twittername'], 'data/%s.csv' % item['twittername'])


if __name__ == "__main__":
    ext = DataExtractor()
    ext.extract_all_raw()


