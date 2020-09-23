import requests
import json
import os
import time
from datetime import datetime as dt

class Deal():

    def __init__(self, limit=100, after = None):

        self.limit = limit
        self.after = after
        self.deals = {}
        self.deals_timeline = []
        self.headers = ['id', 'createdate', 'closedate', 'dealstage', 'hs_analytics_source', 'amount', 'num_notes', 'timestamp']
        self._properties = ['dealstage', 'hs_analytics_source', 'num_notes',
                            'amount', 'createdate', 'closedate']
        self._propertiesWithHistory = ['closedate', 'dealstage', 'amount', 'num_notes']
        self._dealstage = self._fetchDealStageLabels()


    def fetchNext(self):

        if self.after == '':
            return None

        response = self._APIConnectionV3()

        for result in response['results']:
            deal = result['properties']
            if deal['createdate'] is not None:
                deal['createdate'] = self._formatDate(deal['createdate'])
            if deal['closedate'] is not None:
                deal['closedate'] = self._formatDate(deal['closedate'])
            deal_properties = {k:deal[k] for k in self._properties}
            deal_properties['dealstage'] = self._dealstage[deal_properties['dealstage']]
            self.deals[result['id']] = deal_properties

        try:
            self.after = response['paging']['next']['after']
        except KeyError:
            self.after = ''
            print('All deals returned!')

    def generateHistory(self, list_=False):

        count = 0
        for deal_id in self.deals.keys():
            if (count%100 == 0):
                time.sleep(10)
            self._fetchDealHistory(deal_id)
            count =+ 1

        for deal_id, deal in self.deals.items():
            self._treatHistory(deal_id,deal, list_)

        if list_:
            self.deals_timeline = self._dealTimelineToList()


    def _APIConnectionV3(self):

        key = os.environ['HUBSPOT_KEY']
        url = 'https://api.hubapi.com/crm/v3/objects/deals'
        querystring = {'limit':str(self.limit),
                       'archived':'false',
                       'properties':','.join(self._properties),
                       'hapikey':key,
                       'after':self.after}
        headers = {'accept': 'application/json'}
        response = requests.request('GET', url, headers=headers, params=querystring)
        
        return json.loads(response.text)

    def _APIConnectionV1(self, deal_id):

        key = os.environ['HUBSPOT_KEY']
        url = 'https://api.hubapi.com/deals/v1/deal/' + str(deal_id)
        querystring = {'hapikey':key, 'includePropertyVersions':'true'}
        headers = {'accept': 'application/json'}
        response = requests.request('GET', url, headers=headers, params=querystring)
        
        try:
            json.loads(response.text)
        except:
            return None

        return json.loads(response.text)

    def _fetchDealHistory(self, deal_id):

        response = self._APIConnectionV1(deal_id)
        if response is None:
            return None
        
        for prop in self._propertiesWithHistory:
            try:
                self.deals[str(deal_id)][prop] = response['properties'][prop]
            except KeyError:
                continue
    
    def _treatHistory(self, deal_id, deal, list_):

        prop_value_ts = {}
        unique_date = set()

        for prop, value in deal.items():

            if not isinstance(value, dict):
                continue

            prop_value_ts[prop]= []                

            try:
                prop_value_ts[prop].append((value['value'], self._formatTimestamp(value['timestamp'])))
                unique_date.add(dt.date(self._formatTimestamp(value['timestamp'])))
                for version in value['versions']:
                    prop_value_ts[prop].append((version['value'], self._formatTimestamp(version['timestamp'])))
                    unique_date.add(dt.date(self._formatTimestamp(version['timestamp'])))
            except KeyError:
                continue

        for prop in prop_value_ts.values():
            prop.sort(key=lambda prop:prop[1])

        first_date = sorted(list(unique_date))[0]

        for date in unique_date:

            deal_pic = {'timestamp':date,
                        'id':deal_id,
                        'createdate':deal['createdate'],
                        'hs_analytics_source':deal['hs_analytics_source'],
                        'closedate':None,
                        'dealstage':None,
                        'amount':None,
                        'num_notes':None}

            for prop, versions in prop_value_ts.items():
                old_date = first_date
                for version in versions:
                    if old_date <= version[1].date() and version[1].date() <= date:
                        deal_pic[prop] = version[0]

            if deal_pic['amount'] == '':
                deal_pic['amount'] = None
            if deal_pic['amount'] is not None:
                deal_pic['amount'] = float(deal_pic['amount'])
            if deal_pic['closedate'] is not None:
                deal_pic['closedate'] = dt.date(self._formatTimestamp(deal_pic['closedate']))
            if deal_pic['dealstage'] is not None:
                deal_pic['dealstage'] = self._dealstage[deal_pic['dealstage']]

            if list_:
                deal_pic['timestamp'] = deal_pic['timestamp'].strftime('%d/%m/%Y')
                deal_pic['createdate'] = deal_pic['createdate'].strftime('%d/%m/%Y')
                if deal_pic['closedate'] is not None:
                    deal_pic['closedate'] = deal_pic['closedate'].strftime('%d/%m/%Y')


            self.deals_timeline.append(deal_pic) 

    def _fetchDealStageLabels(self):

        key = os.environ['HUBSPOT_KEY']
        url = 'https://api.hubapi.com/crm/v3/pipelines/deals'
        querystring = {'hapikey':key}
        headers = {'accept': 'application/json'}
        response = requests.request('GET', url, headers=headers, params=querystring)

        results = json.loads(response.text)['results'][0]
        return {stage['id']:stage['label'] for stage in results['stages']}

    def _formatDate(self, date):
        return dt.strptime(date[:10], '%Y-%m-%d').date()

    def _formatTimestamp(self, timestamp):
        return dt.fromtimestamp(int(timestamp)/1000)

    def _dealTimelineToList(self):
        return [[deal[item] for item in self.headers] for deal in self.deals_timeline]