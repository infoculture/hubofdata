# -*- coding: utf8 -*-


import sys, os, mechanize
import HTMLParser
from StringIO import StringIO
from BeautifulSoup import BeautifulSoup
EMISS_URL = 'http://www.fedstat.ru/indicators/org.do?id=62'
from lxml import etree
from urllib import unquote
unescape = HTMLParser.HTMLParser().unescape

#keys = ['year', 'month', 'reg_name', 'ind_robbery', 'ind_rape', 'ind_steal', 'ind_drugs', 'ind_peculation', 'ind_brigadary', 'ind_death', 'ind_harm', 'ind_ruffian']
def get_indicators():
    br = mechanize.Browser()
    resp = br.open(EMISS_URL)
    data = resp.read()
#    print data
    text = data.split("setXMLString('", 1)[1]
    text = text.split("')", 1)[0]
#    text = unescape(text)
    print text
    
#    text = unquote(text)
#    print text.encode('utf8')
    doc = etree.parse(StringIO(text))
#    print doc 
    folders = doc.xpath('//folder')
    for f in folders:
	print unescape(f.attrib['text'])
	print f.xpath('//*')
    print '------'
    print etree.tostring(doc, pretty_print=True)
#    print etree.tostring(doc)
if __name__ == "__main__":
    get_indicators()    