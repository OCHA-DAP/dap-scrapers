import endtoend
import os
import sys
import datetime
NOW = datetime.datetime.now().isoformat()
PATH = sys.argv[1]

for fname in os.listdir(PATH):
    print fname
    filepath = os.path.join(PATH, fname)
    country = fname.partition('.')[0]
    resource_info = {
        "package_id": "6bc3b7d7-ff4d-42f6-b689-d92bc4842050",
        "revision_id": NOW,
        "description": "Indicators scraped from a variety of sources by ScraperWiki - by country - {}".format(country),
        "format": "csv",
        # "hash": None,
        "name": country,
        # "resource_type": None,
        "mimetype": "application/zip",
        "mimetype_inner": "text/csv",
        # "webstore_url": None,
        # "cache_url": None,
        # "size": None,
        "created": NOW,
        "last_modified": NOW,
        # "cache_last_updated": None,
        # "webstore_last_updated": None,
        }
    endtoend.upload(resource_info=resource_info, filename=filepath)
