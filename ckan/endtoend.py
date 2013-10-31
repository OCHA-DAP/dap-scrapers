"""
curl $CKAN_INSTANCE/api/storage/auth/form/$DIRECTORY/$FILENAME -H Authorization:$CKAN_APIKEY > phase1
curl $CKAN_INSTANCE/storage/upload_handle -H Authorization:$CKAN_APIKEY --form file=@$FILENAME --form "key=$DIRECTORY/$FILENAME" > phase2
curl http://ckan.megginson.com/api/3/action/resource_create --data '{"package_id":"51b25ca0-9c2e-4e66-85e3-37a13c19a85d", "url":"'$CKAN_INSTANCE'/storage/f/'$DIRECTORY'/'$FILENAME'"}' -H Authorization:$CKAN_APIKEY > phase3
"""

import sys
import os
import requests
import datetime
import lxml.html
import json

def get_parameters():
    params = {}
    params['CKAN_INSTANCE'] = os.getenv("CKAN_INSTANCE")
    params['CKAN_APIKEY'] = os.getenv("CKAN_APIKEY")
    if not params['CKAN_INSTANCE'] or not params['CKAN_APIKEY']:
        raise RuntimeError("Enviroment variables CKAN_INSTANCE / CKAN_APIKEY not set.")

    if len(sys.argv) != 2:
        raise RuntimeError("Takes one argument: filename")
    else:
        params['FILEPATH'] = sys.argv[1]
        params['FILENAME'] = os.path.basename(params['FILEPATH'])

    params['DIRECTORY'] = datetime.datetime.now().isoformat().replace(":", "").replace("-", "")
    return params

params = get_parameters()
headers = {"Authorization": params['CKAN_APIKEY']}

def request_permission():  # phase1
    response = requests.get("{CKAN_INSTANCE}/api/storage/auth/form/{DIRECTORY}/{FILENAME}".format(**params), headers=headers)
    response.raise_for_status()
    j = response.json()
    assert "action" in j
    assert "fields" in j
    print j
    return j

def upload_file(permission):  # phase 2
    response = requests.post("{CKAN_INSTANCE}{action}".format(action=permission['action'], **params),
                             headers=headers,
                             files={'file': (params['FILENAME'], open(params['FILEPATH']))},
                             data={permission['fields'][0]['name']: permission['fields'][0]['value']}
                             )
    response.raise_for_status()
    root = lxml.html.fromstring(response.content)
    h1, = root.xpath("//h1/text()")
    assert " Successful" in h1
    url, = root.xpath("//h1/following::a[1]/@href")
    assert params['FILENAME'] in url  # might be issues with URLencoding
    return url

def create_resource(url, **kwargs):  # phase 3
    data = {"url": url,
            "package_id": "51b25ca0-9c2e-4e66-85e3-37a13c19a85d"}
    newheader = dict(headers)
    newheader['Content-Type'] = "application/x-www-form-urlencoded"  # http://trac.ckan.org/ticket/2942
    data.update(kwargs)
    print data
    response = requests.post("{CKAN_INSTANCE}/api/3/action/resource_create".format(**params),
    # response = requests.post("http://httpbin.org/post".format(**params),
                             headers=newheader,
                             data=json.dumps(data)
                             )
    #response.raise_for_status()
    #assert response.json()["success"]
    print response.content

j = request_permission()
url = upload_file(j)
create_resource(url)
