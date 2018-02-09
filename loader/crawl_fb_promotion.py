# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import datetime
import requests
import traceback
from retrying import retry


# 添加整个工程的path
django_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not django_path in sys.path:
    sys.path = [django_path]+sys.path
# 添加django工程的path
django_path = os.path.join(django_path, 'adsense')
if not django_path in sys.path:
    sys.path = [django_path]+sys.path

from django.core.wsgi import get_wsgi_application
# 设置django的setting路径
os.environ['DJANGO_SETTINGS_MODULE'] = 'adsense.settings'
# 初始化django application
application = get_wsgi_application()

from api.models import *

from utils import send_email
# bdp settings
BDP_SCHEMA = [
    {'name': 'Date',          'remark': '',  'type': 'date'},
    {'name': 'Code',          'remark': '',  'type': 'string'},
    {'name': 'Account ID',    'remark': '',  'type': 'string'},
    {'name': 'Account Name',  'remark': '',  'type': 'string'},
    {'name': 'Impressions',   'remark': '',  'type': 'number'},
    {'name': 'Clicks',        'remark': '',  'type': 'number'},
    {'name': 'Conversions',   'remark': '',  'type': 'number'},
    {'name': 'Reach',         'remark': '',  'type': 'number'},
    {'name': 'Cost',          'remark': '',  'type': 'number'},
]
BDP_UNIQ_KEY = ['Date', 'Code', 'Account ID']
BDP_SCHEMA_FIELDS = [item['name'] for item in BDP_SCHEMA]

from opends.sdk import BDPClient

def get_bdp_table(access_token, ds_name, table_name, schema, uniq_key):
    try:
        client = BDPClient(access_token)
        ds = client.get_ds(ds_name)
    except Exception as e:
        print e
        ds = client.create_ds(ds_name)
    try:
        table = ds.get_table(table_name)
    except Exception as e:
        print e
        table = ds.create_table(table_name, schema=schema, uniq_key=uniq_key)
    return ds, table


def crawl_fb_promotion_data(account):
    print '=# '*20
    print account
    items = []
    url = 'https://graph.facebook.com/v2.10/act_{}/insights'.format(account.account_id)
    print url
    # yesterday
    params = {
        'fields': 'account_name,impressions,clicks,spend,reach,actions,cost_per_action_type',
        'date_preset': 'yesterday',
        'access_token': account.token,
    }
    for day in xrange(7):
        day = (datetime.datetime.now()-datetime.timedelta(days=day)).strftime('%Y-%m-%d')
        time_range = '{"since": "%s", "until": "%s"}' %(day, day)
        params['time_range'] = time_range
        resp = requests.get(url, params=params)
        print resp.status_code
        print resp.content
        if resp.status_code/100 <> 2:
            raise Exception('crawl fb promotion data failed. status_code: %d, %s' % (resp.status_code, resp.content))
        resp_json = resp.json()['data']
        if resp_json:
            data = resp.json()['data'][0]
            installs = 0
            if 'actions' in data:
                for item in data['actions']:
                    if item['action_type'] == 'mobile_app_install':
                        installs = int(item['value'])
            items.append({
                'Date': data['date_start'],
                'Code': account.code,
                'Account ID': account.account_id,
                'Account Name': data['account_name'],
                'Impressions': int(data['impressions']),
                'Clicks': int(data['clicks']),
                'Conversions': installs,
                'Reach': int(data['reach']),
                'Cost': float(data['spend']),
            })
    return items


@retry(stop_max_attempt_number=20, wait_fixed=10000)
def update_to_bdp(ds, table, fields, data):
    table.update_data_by_name(fields, data)
    table.commit()
    ds.update([table.get_id()])


def crawl():
    try:
        bdp_data = []
        for account in FBPromotionModel.objects.filter(active=True):
            for item in crawl_fb_promotion_data(account):
                bdp_data.append([item[key] for key in BDP_SCHEMA_FIELDS])
        print bdp_data
        with open('fb_promotion.log', 'wb') as f:
            f.write(json.dumps(bdp_data))
        # 更新到bdp
        if bdp_data:
            ds, table = get_bdp_table(
                access_token='xx',
                ds_name='product',
                table_name='fb_promotion',
                schema=BDP_SCHEMA,
                uniq_key=BDP_UNIQ_KEY
            )
            print table
            update_to_bdp(ds, table, BDP_SCHEMA_FIELDS, bdp_data)
    except Exception as e:
        print e
        traceback.print_exc()
        etype, value, tb = sys.exc_info()
        tb_value = ''
        for i in traceback.format_tb(tb):
            tb_value += '<br>{}'.format(i)
        text = '''
            <html>
                <body>
                    <a>Hi All,<a>
                    <br><br>
                    错误信息:
                    <br>
                    {}
                    <br>
                    {}
                    {}
                </body>
            </html>
        '''.format(str(etype), value, tb_value)
        subject = 'Crawl FB Promotion Error-- {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time())))
        send_email(subject=subject, text=text, to='xx@yy.com')


if __name__ == '__main__':
    crawl()