#!/usr/bin/env python
# coding: utf8
"""
Importer data taken from Clearspending data to the opendata hub
"""
import ckanclient
import csv
import json

API_URL = 'http://hubofdata.ru/api'
API_KEY_FILENAME = "apikey.txt"
DATASETS_FILENAME = 'merged.csv'
CSV_URLPAT = 'http://cdn1.sdlabs.ru/spending/%s/%s'

class DataImporter:
    """Clearspending data"""
    def __init__(self):
        self.apikey = open(API_KEY_FILENAME).read()
        self.ckan = ckanclient.CkanClient(base_location=API_URL, api_key=self.apikey)
        self.package_list = self.ckan.package_register_get()
        self.started = False
        self.start_key = ''
        pass

    def delete_all(self):
        """Delete all data from datasets.csv and creates package"""
        f = open(DATASETS_FILENAME, 'r')
        data = f.read()
        f.close()
        reader = json.loads(data)
        package_names = []
        for package in reader['aaData']:
            id = package['resourceId']
            key = 'esimo_' + id.lower()
            try:
                self.ckan.package_entity_delete(key)
                print key, 'deleted'
            except Exception, e:
                print key, 'not deleted. Error'

        pass


    def import_all(self):
        """Processes all data from datasets.csv and creates package"""
        f = open(DATASETS_FILENAME, 'r')
        reader = csv.DictReader(f)
        package_names = []
        n = 0
        for package in reader:
            package_names.append(self.register(package))
            n += 1
            print "Adding package", n
        self.update_group('clearspending', package_names)
        f.close()
        pass

    def register(self, package):
        """Register or update dataset
        :param package:
        """
        id = package['code'].replace('.', '_')
        key = 'gzp_' + id.lower()

        try:
            r = self.ckan.package_entity_get(key)
            status = 0
        except ckanclient.CkanApiNotFoundError, e:
            status = 404

        tags = [u'госзакупки', u'ОКПД %s' % (package['code']), u'цены']
        name = package['name'] + u': цены из госконтрактов за 2014 и 2015 годы'.encode('utf8')
        resources = [{'name': name, 'format': 'CSV', 'url': CSV_URLPAT % ('2014', package['files_2014']),
                      'description': u'Данные в формате CSV за 2014 год'},
                     {'name': name, 'format': 'CSV', 'url': CSV_URLPAT % ('2015', package['files_2015']),
                                   'description': u'Данные в формате CSV за 2015 год'},
        ]
        print name
        extras = {'code' : package['code'],
                  'name' : package['name'],
        }



        the_package = { 'name' : key, 'title' : name,
                           'notes' :  (u'Данные по всем предметам контрактов по коду классификации ОКПД: %s (%s) за 2014 и 2015 год из базы портала Госзатраты http://clearspending.ru\n\nОписание данных в Вики: https://github.com/idalab/clearspending-examples/wiki \n\n' % (package['code'].decode('utf8'), package['name'].decode('utf8'))).encode('utf8'),
                           'tags' : tags,
                           'state' : 'active',
                           'resources': resources,
                           'author' : 'Ivan Begtin',
                           'author_email' : 'ibegtin@infoculture.ru',
                           'maintainer' : 'Ivan Begtin',
                           'maintainer_email' : 'ibegtin@infoculture.ru',
                           'license_id' : 'other-nc',
                           'owner_org': u'infoculture',
                           'extras': extras
                        }
#        print the_package
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
