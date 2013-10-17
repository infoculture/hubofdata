#!/usr/bin/env python
# -*- coding: utf8 -*-

from BeautifulSoup import BeautifulStoneSoup
import csv, os, sys, urllib2
from bsoupxpath import Path
import chardet
import html5lib
from html5lib import treebuilders



def get_indicators():
	filename = 'refined/tables.csv'
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	f = open('scripts/get_indicators.sh', 'w')
	f.write('#bin/sh\n')
	for r in csvr:
		tkey = r['ind_table']
		f.write('mono cbrexporter.exe indicators %s > ../raw/indicators/%s.xml\n' %(tkey, tkey))
	f.close()


def get_values():
	filename = 'refined/tables.csv'
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	filename = 'refined/regions.csv'
	regs = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	tables = []
	for o in csvr:
		tables.append(o)
	inds = []
	filename = 'refined/indicators.csv'
	for o in csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE):
		inds.append(o)
#	f = open('scripts/get_values.bat', 'w')
	for r in regs:
		rkey = r['reg_id']
#		f.write('mkdir ..\\raw\\values\\%s' %(rkey)+ '\n')
		for  t in tables:
			tkey = t['ind_table']
			ires = []
			for i in inds:
				if i['ind_table'] == tkey:
					ires.append(i['ind_id'])
			efilename = 'raw/values/%s/%s__%s.xml' %(rkey, rkey, '_'.join(ires))
			if not os.path.exists('raw/values/%s/' %(rkey)):
				os.makedirs('raw/values/%s' %(rkey))
			if not os.path.exists(efilename):			
				s = 'mono scripts/cbrexporter.exe values %s %s raw/values/%s/%s__%s.xml' %(rkey, ','.join(ires), rkey, rkey, '_'.join(ires))
				print 'Extracting', efilename
				os.system(s)
			else:
				print 'Skipped', efilename
#			f.write(s + '\n')
#	f.close()
	

if __name__ == "__main__":
#	get_indicators()
	get_values()
	
