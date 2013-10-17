#!/usr/bin/env python
# coding: utf8
"""
Importer data taken for party reports to the opendata hub
"""
import ckanclient
import csv
import json
import hashlib
API_KEY_FILENAME = "apikey.txt"

API_URL = "http://hubofdata.ru/api"
CATALOG_FILE = 'data/datasets.csv'
DIRECT_DOWNLOAD_URLPAT = "http://d2f6aadeaff96aaafda4-614b9ac7aa1f2556da9aa7df0eee2346.r98.cf2.rackcdn.com/cikrf/data/raw/%s"


class DataImporter:
    """Data importer class for data.mos.ru"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        self.started = True
        self.start_key = ''
        self.tags = [u'политика', u'выборы', u'политические партии', u'финансы']
        self.package_keys = { 'govbody': u'ЦИК России'}
        pass



    def import_all(self):
        """Processes all data from datasets.csv and creates package"""
        reader = csv.DictReader(open(CATALOG_FILE, 'r'), delimiter="\t")
        package_names = []
        for package in reader:
            filename = package['url'].rsplit('/')[-1].lower()
            ext = filename.rsplit('.')[-1]
            if ext not in ['xls', 'xlsx']: continue
            package_names.append(self.register(package))
        self.update_group('politics', package_names)
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        filename = package['url'].rsplit('/')[-1]

        id = hashlib.sha1(filename).hexdigest().encode('utf-8')[0:8]
        key = 'cikrf' + id.lower()
        if not self.started:
            if key != self.start_key:
                print 'Already imported. Skipping'
                return key
            else:
                self.started = True


        try:
            r = self.ckan.package_entity_get(key)
            status = 0

        except ckanclient.CkanApiNotFoundError, e:
            status = 404

        name = package['name'] + ' (%s)' % package['category_name']
        tags = []
        for k in range(2004, 2014):
            sk = str(k)
            if name.find(sk) > -1:
                tags.append(sk)
        tags.extend(self.tags)
        resources = [{'name': name, 'format': 'XLS', 'url': DIRECT_DOWNLOAD_URLPAT % filename,
                      'description': u'Данные в формате XLS выгруженные с сайта ЦИК России'},
                     {'name': package['category_name'], 'format': '', 'url': package['category_url'],
                      'description': u'Страница отчетов на сайте ЦИК России'}
        ]
        print name
        package.update(self.package_keys)

        the_package = { 'name' : key, 'title' : name,
                           'notes' :  package['name'] + u'\n\nДанные с ЦИК России - cikrf.ru.\n\n'.encode('utf8'),
                           'tags' : tags,
                           'state' : 'active',
                           'resources': resources,
                           'author' : 'Ivan Begtin',
                           'author_email' : 'ibegtin@infoculture.ru',
                           'maintainer' : 'Ivan Begtin',
                           'maintainer_email' : 'ibegtin@infoculture.ru',
                           'license_id' : 'other-nc',
                           'owner_org': "",
                           'extras': package
                        }
#            self.ckan.package_entity_update(package)
#
#        if True:
        if status == 404:
            try:
                self.ckan.package_register_post(the_package)
            except Exception, e:
                print 'Error importing', key
                print e, type(e), e.message
                return key
                pass
            print "Imported", key
        else:
            package_entity = self.ckan.last_message
            if type(package_entity) == type(''): return None
            package_entity.update(the_package)
            for k in ['id', 'ratings_average', 'relationships', 'ckan_url', 'ratings_count']:
                del package_entity[k]
            self.ckan.package_entity_put(package_entity)
            print "Updated", key
#            print self.ckan.last_message
        return key

    def update_group(self, group_name, package_names, group_title="", description=""):
            #        print key
            try:
                group = self.ckan.group_entity_get(group_name)
                status = 0
            except ckanclient.CkanApiNotFoundError, e:
                status = 404
            if status == 404:
                group_entity = {'name' : group_name, 'title' : group_title, 'description' : description }
                self.ckan.group_register_post(group_entity)
            group = self.ckan.group_entity_get(group_name)
            for name in package_names:
                if name not in group['packages']:
                    group['packages'].append(name)
            self.ckan.group_entity_put(group)


if __name__ == "__main__":
    imp = DataImporter()
#    imp.collect_data()
#    imp.delete_all()
    imp.import_all()