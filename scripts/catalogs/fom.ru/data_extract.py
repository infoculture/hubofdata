#!/usr/bin/env python
# coding: utf8
"""
Fom.ru extractors

"""
import csv
import os, os.path
from urllib2 import urlopen
import urllib2
import xlrd
import  json
from decimal import Decimal

BASE_URL = 'http://fom.ru'
FILEPATH_RAW = 'data/raw/'
EXTRACTED_PATH = 'data/extracted/'
FILEPATH_PROCESSED = 'data/processed/'
CATALOG_FILE = 'data/articles.csv'
DATARANGE = range(11043, 10000, -1)
URLPATTERN = 'http://fom.ru/posts/%s'
from BeautifulSoup import BeautifulSoup

class DataExtractor:
    """Data extractor from fom.ru  """

    def __init__(self):
        pass

    def store_file(self, id, url):
        data = urlopen(url).read()
        f = open(FILEPATH_RAW + '%s.xlsx' % (id), 'wb')
        f.write(data)
        f.close()
        print 'Stored', '%s.xlsx' % (id)


    def process_all(self):
        all = {}
        if os.path.exists(CATALOG_FILE):
            reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
            i = 0
            for item in reader:
                all[item['id']] = item
            f = open(CATALOG_FILE, 'a')
        else:
            f = open(CATALOG_FILE, 'w')
            keys = ['id', 'url', 'status']
            s = ('\t'.join(keys)).encode('utf8') + '\n'
            f.write(s)

        for i in DATARANGE:
            key = str(i)
            if key in all.keys():
                print 'Skipping', key
                continue
            print 'Processing', key
            try:
                data = urlopen(URLPATTERN % key)
            except urllib2.HTTPError:
                status = 'nopage'
                r = [key, URLPATTERN % key, status]
                s = ('\t'.join(r)).encode('utf8') + '\n'
                f.write(s)
                print 'Page not found', key
                continue
            soup = BeautifulSoup(data)
            a = soup.find('a', attrs={'class' : 'download_data d-data-1'})
            if a is not None:
                status = 'available'
                self.store_file(key, BASE_URL, a['href'])
            else:
                status = 'nodata'
                print 'Data not found', key
            r = [key, URLPATTERN % key, status]
            s = ('\t'.join(r)).encode('utf8') + '\n'
            f.write(s)


    def convert_all_to_csv(self):
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        i = 0
        for item in reader:
            if item['status'] != 'available':
                continue
            print 'Processing', item['id']
            try:
                wb = xlrd.open_workbook(FILEPATH_RAW + item['id'] + '.xlsx')
            except:
                print 'Error on', item['id'], 'skipped'
            sh = wb.sheet_by_index(0)
            your_csv_file = open(FILEPATH_PROCESSED + item['id'] + '.csv', 'wb')
            wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)

            for rownum in xrange(sh.nrows):
                values = sh.row_values(rownum)
                row = []
                for v in values:
                    row.append(unicode(v).encode('utf8'))
                wr.writerow(row)

            your_csv_file.close()
            print 'Converted', item['id']

    def extract_json_data(self):
        """Extracts social groups and assigns uniq id"""
        allsurveys = []
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        for item in reader:
            survey = {}
            if item['status'] != 'available':
                continue
            print 'Processing', item['id']
            your_csv_file = open(FILEPATH_PROCESSED + item['id'] + '.csv', 'r')
            reader = csv.reader(your_csv_file, quoting=csv.QUOTE_ALL)
            i = 0
            groups = []
            allrows = []
            for row in reader:
                allrows.append(row)
            questions = []
            subrow_num = 0
            uniq_g_id = 1
            uniq_sg_id = 1
            title = ""
            description = ""
            title_found = False
            description_found = False
            share_found = True
            for row in allrows:
                i += 1
                if len(row) > 0 and len(row[0]) > 0:
                    if not title_found:
                        title = row[0]
                        title_found = True
                        continue
                    elif not description_found:
                        description = row[0]
                        description_found = True
                        continue
                if len(row) > 1 and row[1].decode('utf8') == u'Население   в целом':
                    toprow = row
                    subrow = allrows[i]
                    if allrows[i+1][0].decode('utf8') == u'Доли групп':
                        sharerow = allrows[i+1]
                        share_found = True
                    else:
                        share_found = False
                        sharerow = None
                    n = 0
                    subrow_num = i
                    group = None
                    for k in toprow:
                        n += 1
                        if len(k) > 0:
                            if group is not None:
                                uniq_g_id += 1
                                groups.append(group)
                            group = {'gid': 'g_%d' % uniq_g_id, 'name': k, 'subgroups' : []}
                            sg = {'sid' : '%d' % uniq_sg_id, 'name': subrow[n-1] if len(subrow[n-1]) > 0 else k}
                            if sharerow:
                                sg['share'] = sharerow[n-1]
                            group['subgroups'].append(sg)
                            uniq_sg_id += 1
                        else:
                            if group is not None and len(subrow[n-1]) > 0:
                                sg = {'sid' : '%d' % uniq_sg_id, 'name': subrow[n-1] if len(subrow[n-1]) > 0 else k}
                                if sharerow:
                                    sg['share'] = sharerow[n-1]
                                group['subgroups'].append(sg)
                                uniq_sg_id += 1
                    if group is not None:
                        groups.append(group)
                        uniq_g_id += 1

                    break
            q_uniq_id = 1
            a_uniq_id = 1
            question = None
            answers = []
            if share_found:
                SHIFT = 2
            else:
                SHIFT = 1
            i = subrow_num + SHIFT
            for row in allrows[subrow_num + SHIFT:]:
                i += 1
#                if len(row[0]) > 0:
#                    print 'Row', i, row[0].strip()
                if len(row) > 0 and row[0].strip().decode('utf8') in [u'Открытый вопрос', u'Открытые вопросы']:
                    print '# open questions found. Break'
                    break
                if (len(row) > 1 and len(row[1]) == 0 and len(row[0]) > 0) or len(row) == 1:
                    if question is not None:
#                        print answers
                        questions[-1]['answers'] = answers
                    answers = []
                    question = {'qid': '%d' % q_uniq_id, 'name' : row[0]}
                    questions.append(question)
                    q_uniq_id += 1
                else:
                    try:
                        n = float(row[1])
                        a_uniq_id += 1
                        answer = {'aid' : a_uniq_id, 'name' : row[0]}
                        values = []
                        for v in row[1:]:
                            values.append(v)
                        answer['values'] = values
                        if question is not None:
                            answers.append(answer)
                    except:
#                        print row[1]
                        continue
            questions[-1]['answers'] = answers
            survey['id'] = item['id']
            survey['url'] = item['url'][len(BASE_URL):]
            survey['title'] = title
            survey['description'] = description
            survey['questions'] = questions
            survey['groups'] = groups
            print survey['title']
            f = open(EXTRACTED_PATH + item['id'] + '.json', 'w')
            f.write(json.dumps(survey, indent=4))
            f.close()
            allsurveys.append(survey)
        f = open('data/surveys.json', 'w')
        f.write(json.dumps(allsurveys, indent=4))
        f.close()


    def calc_social_groups(self):
        f = open('data/surveys.json', 'r')
        js = json.loads(f.read())
        f.close()
        allgroups = {}
        full = {}
        for item in js:
            for g in item['groups']:
                v = allgroups.get(g['name'], 0)
                allgroups[g['name']] = v + 1
                if g['name'] not in full.keys():
                    full[g['name']] = {'surveys': [item['id'], ]}
                else:
                    full[g['name']]['surveys'].append(item['id'])
        for k in full.keys():
            full[k]['total'] = len(full[k]['surveys'])
        f = open('data/socialgroups.json', 'w')
        f.write(json.dumps(full, indent=4))
        f.close()
#        for k, v in allgroups.items():
#            print k.encode('utf8'), v

    def calc_questions(self):
        f = open('data/surveys.json', 'r')
        js = json.loads(f.read())
        f.close()
        allgroups = {}
        full = {}
        for item in js:
            for g in item['questions']:
                v = allgroups.get(g['name'], 0)
                allgroups[g['name']] = v + 1
                if g['name'] not in full.keys():
                    full[g['name']] = {'surveys': [item['id'], ]}
                else:
                    full[g['name']]['surveys'].append(item['id'])
        for k in full.keys():
            full[k]['total'] = len(full[k]['surveys'])
        f = open('data/questions.json', 'w')
        f.write(json.dumps(full, indent=4))
        f.close()
        res = sorted(allgroups.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
        for k, v in res[:10]:
            print k.encode('utf8'), v

    def calc_stats(self):
        f = open('data/surveys.json', 'r')
        js = json.loads(f.read())
        f.close()
        allgroups = {}
        for item in js:
            for g in item['groups']:
                for s in g['subgroups']:
                    v = allgroups.get(s['name'], 0)
                    allgroups[s['name']] = v + 1
                    print item['id'], s['name']

        for k, v in allgroups.items():
            print k.encode('utf8'), v



if __name__ == "__main__":
    ext = DataExtractor()
#    ext.calc_stats()
#    ext.process_all()
#    ext.convert_all_to_csv()
    ext.extract_json_data()
    ext.calc_social_groups()
    ext.calc_questions()


