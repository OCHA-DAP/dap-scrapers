import requests
import StringIO

cache=True
if cache:
    import requests_cache
    requests_cache.install_cache("cache")

def grab(url, silence=[]):
    r=requests.get(url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError, e:
        num = int(str(e).split(' ')[0])
        if num in silence:
            print num
            return None
        else:
            raise
    return StringIO.StringIO(r.content)

