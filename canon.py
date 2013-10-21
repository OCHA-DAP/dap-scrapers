"""convert country names to M49 codes."""

import dataset
import fileinput
import dedupe
import logging
import sys
import scrumble

db = dataset.connect('sqlite:///canon.db')
region = db['region']
chd = db['chd']

log = logging.getLogger("canon")
log.addHandler(logging.StreamHandler())
log.addHandler(logging.FileHandler("canon.log"))
log.level = logging.WARN


def getpair(text):
    """cheap and dirty CSV parsing"""
    chars = '\t|,'
    for char in chars:
        if char in text:
            return [x.strip() for x in text.split(char)]
    raise RuntimeError("getpair: found none of %r in %r." % (chars, text))


def chd_id(text):
    """conceptually very similar to canon, but for Common Humanitarian Dataset IDs.
       Features significantly less faff due to relying on exact string match."""
    if chd.find_one(sw_name=text):
        return chd.chd_code
    log.warn("No CHD code found for %r" % text)


def canonicalise(rawname):
    """see if there's a matching country in the DB already, give answer"""
    if len(rawname) == 3 and region.find_one(code=rawname.upper()):
        return rawname.upper()
    name = dedupe.apply_one(rawname)
    nicename = region.find_one(name=name)
    if not nicename:
        name = dedupe.apply_one_keep_bracket(rawname)
        nicename = region.find_one(name=name)
        if not nicename:
            log.warn("Name %r (%r) not found." % (rawname, name))
            return None
    return nicename.code


def canon_number(f):
    num = scrumble.as_float(f, strict=True)
    if not num:
        return None
    if not isinstance(num, float):
        log.warn("Unable to transmute %r to float" % f)
        return None
    return num


def canon_period(p):
    if p is None:
        return ''
    if isinstance(p, basestring):
        return p
    assert int(p) == float(p)
    return str(int(p))


def updatedb():
    """update db with data from a file in a CSV-like format"""
    m49 = 'm49' in sys.argv[1]
    chd_in = 'chd' in sys.argv[1]
    for i, line in enumerate(fileinput.input()):
        left, right = getpair(line.decode('utf-8'))
        if not chd_in:
            left = dedupe.apply_one(left)  # convert to a key
        if not m49 and not chd_in:
            right = canonicalise(right)  # convert to One True Name
        if right is not None:
            if not chd_in:
                region.upsert({'name': left, 'code': right}, ['name', 'code'])
            else:
                chd.upsert({'sw_name': left, 'chd_code': right},
                           ['sw_name', 'chd_code'])

if __name__ == "__main__":
    updatedb()


def _ignore():
    if len(sys.argv) > 1:
        m49 = "m49" in sys.argv[1]
        print "parsing ", sys.argv[1:]
        updatedb(m49)
    else:
        for i in open("who-nasty.out").read().split('\n'):
            try:
                canonicalise(i)
            except Exception as e:
                print e

