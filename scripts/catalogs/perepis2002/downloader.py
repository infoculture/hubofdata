# -*- coding: utf-8 -*-

import sys, os, urllib2

from BeautifulSoup import BeautifulSoup
from decimal import Decimal
from pyparsing import Word, nums, alphas, oneOf, lineStart, lineEnd, Optional, restOfLine, Literal, ParseException, CaselessLiteral
import re, os.path, cPickle
import calendar


def download():
	urllist = []
	urllist.append('http://www.perepis2002.ru/ct/doc/_02-01_new.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/02-02_new.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_02-03_.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_02-04_.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_05-01_new.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_05-02_.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_05-03_.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/_05-04_new.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/05-05_new.xls')
	urllist.append('http://www.perepis2002.ru/ct/doc/05-06_new.xls')
	for i in range(1,45):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_14_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1, 14):
		url = 'http://www.perepis2002.ru/ct/doc/1_TOM_01_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,7):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_12_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,5):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_03_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(5,8):
		url = 'http://www.perepis2002.ru/ct/doc/_TOM_03_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,8):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_04_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,9):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_04_2_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,15):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_13_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,14):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_06_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,7):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_07_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,12):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_08_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,3):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_09_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(3,12):
		url = 'http://www.perepis2002.ru/ct/doc/tom_09_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(3,12):
		url = 'http://www.perepis2002.ru/ct/doc/tom_09_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,7):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_11_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,6):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_10_%0.2d.xls' %(i)
		urllist.append(url)
	for i in range(1,6):
		url = 'http://www.perepis2002.ru/ct/doc/TOM_10_%0.2d.xls' %(i)
		urllist.append(url)
	for url in urllist:
		print url
		f = urllib2.urlopen(url)
		data = f.read()
		f.close()
		f = open('raw/%s' %(url.rsplit('/', 1)[1]), 'wb')
		f.write(data)
		f.close()

def main():
	download()

if __name__ == '__main__':
	main()