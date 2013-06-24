import collections
import csv
import re
from collections import defaultdict, Counter
import doctest

from os.path import abspath, dirname, join
COMPANY_LIST = abspath(join(dirname(__file__), '..', '..', 'fixtures',
                       'emap_list.txt'))

company_index={}
def get_one(it):
    """return first item from iterator
    >>> get_one([1,2,3])
    1
    >>> get_one(set([1,2,3])) in set([1,2,3])
    True
    """
    for i in it:
        return i

def buildindex(s):
    """make a lookup from original name to current key
    >>> buildindex ({'key1': set(['a','b'])})
    {'a': 'key1', 'b': 'key1'}
    """
    index={}
    for row in s:
        assert isinstance(s[row],set) or isinstance(s[row], frozenset)
        for original in s[row]:
            index[original] = row
    return index

def multi(function, s, verbose=False, diff=False, keychange=True, p=True):
    """This version differs in that it attempts to preserve the original names in a dictionary"""
    if not keychange:
        index = buildindex(s)

    new = defaultdict(set)
    if not isinstance(s, dict):
        s = {x:set([x]) for x in s}
    for i in s:
        showdiff = False
        x = function(i)
        if verbose and x!=i: print i, "=>", x
        if diff and x in new: showdiff = True
        assert type(s[i])
        new[x]=new[x].union(s[i])
        if showdiff: print new[x]
    if p: print "%20s %5d %5d %0.4f"%(function.__name__, len(s), len(new), 1.0-(1.0*len(new))/len(s))

    if not keychange:
        """change keys back - this really ought to preserve multiple keys!"""
        q={}
        for row in new:
           companies = new[row]
           company = get_one(companies)
           lookup = index[company]
           q[lookup]=frozenset(companies) # feels too magical.
        return q
    return new



def identity(i):
    return i

def purgebracket(i):
    """
    >>> purgebracket("cat (not really)")
    'cat '
    """
    return re.sub('\([^\(\)]*\)','',i)

def nopunct(i):
    """
    >>> nopunct("cat (not really)")
    'cat not really'
    """
    return re.sub('[^\w ]','',i)

def ampersand(i):
    """
    >>> ampersand("Engl& Fish & Chips")
    'England Fish and Chips'
    """
    return re.sub('&','and',i)

def lcase(i):
    """
    >>> lcase("Cat")
    'cat'
    """
    return i.lower()

def purgewords(i):
    """
    >>> purgewords("the agency of national trust buildings")
    'buildings'
    """
    purge = ["ltd", "limited", "company","","the","of","uk","nhs","pct","foundation","trust",
             "national","department","dept","agency","and","association","authority","co",
             "metropolitan","british","consulting","group","services","systems"]
    words = i.split(' ')
    for p in purge:
        while p in words:
                words.remove(p)
    return ' '.join(words)

def sorted_words(i):
    """
    >>> sorted_words("the cat sat on the mat")
    'cat mat on sat the the'
    """
    words = i.split(' ')
    if len(words)==1:
        return i
    else:
        return ' '.join(sorted(words)).strip()

def purgechars(i, purge):
    """
    >>> purgechars("mississippi", 'sx')
    'miiippi'
    """
    for p in purge:
        i=re.sub(p, '', i)
    return i

def purge_s(i):
    return purgechars(i,'s')

def purge_space(i):
    return purgechars(i, ' ')


def anagram(i):
    """
    really, sorts string into an anagram key
    >>> anagram("cat")
    'act'
    """
    return ''.join(sorted(i)).strip()

"""Function dependencies:
identity isn't required, just nice to set() things
amperstand needs to be before purgewords (ENGL&), purgechars, anagram. Nice and early.
lcase is a requirement for purgewords, since those words are in lowercase
purgebracket must be before nopunct, anagram"""

functionlist = [identity, ampersand, lcase, purgewords, purgebracket, nopunct, purge_space, purge_s]#, anagram]
difflist = [purgebracket]
difflist=[]#functionlist
keepkey = [anagram, purge_s]

def apply(companies):
    """
    So long as it looks like this, ish, we're okay. Dictionary names and the order of everything
    are up for grabs
    >>> rval = apply(['cat', '*cat', 'dog', '*dog', 'wolf']) #doctest: +ELLIPSIS
    >>> rval
    {'wolf': frozenset(['wolf']), 'dog': frozenset(['*dog', 'dog']), 'cat': frozenset(['*cat', 'cat'])}
    """
    for f in functionlist:
        companies = multi(f, companies, diff = f in difflist, keychange = not f in keepkey)
    return companies

def apply_one(name):
    for f in functionlist:
        name = f(name)
    return name

def merge(names):
    out = collections.defaultdict(set)
    for name in names:
        out[apply_one(name)].add(name)
    return out

def show_merged(m, threshold=1):
    "takes output from merge() - shows elided names!"
    for item in sorted(m):
        if len(m[item])>threshold:
            print
            for i in m[item]:
                print i


def demofile():
    names = []
    with open("../old-folder/full.csv", 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            names.append(row[8])
    return names

def best_index(s, master=[]):
    best = {}
    for row in s:
        master_match = [candidate for candidate in s[row] if candidate in master]
        if not len(master_match) < 2: print "WARNING: ", master_match
        if master_match:
            match=master_match[0]
        else:
            match=get_one(s[row])
        for candidate in s[row]:
            best[candidate]=match
    return best

def best_index_wrapper(candidates, master):
    """
    returns dict of candidate: true name
    >>> best_index_wrapper(["*cat", "cat", "*dog", "+dog"], ["dog", "wolf"])
    None
    """
    all_companies = []
    all_companies.extend(candidates)
    all_companies.extend(master)
    s = apply(all_companies)
    best = best_index(s, master)
    return best

def initialise_company_names(candidates):
    global company_index
    master = open(COMPANY_LIST, 'r').read().split('\n')
    company_index = best_index_wrapper(candidates, master)

def companies_wrapper(candidates, master):
    "returns candidates, only deduped"
    best = best_index_wrapper(candidates, master)
    return [best[c] for c in candidates]

def magic_ident(item, cat):
    """
    >>> magic_ident('xxboroughhospitalxx', 'nhs')
    True
    >>> magic_ident('xxboroughhospitalxx', 'ltd')
    False
    """
    magic = {"nhs": ['pct','trust','hospital','nhs','foundation','hospice','sha','ambulance'],
             "pub": ['borough','council','police','unitary'],
             "ltd": ['ltd','limited','plc','llp','corporation'],
             "odd": ['university','society','cic','royal','institut']
            }
    key = nopunct(purgebracket(lcase(ampersand(item))))
    return any (i in key for i in magic[cat])

def is_public_sector(item):
    """
    >>> is_public_sector('Basingstoke Council')
    True
    >>> is_public_sector('Council Limited')
    False
    >>> is_public_sector('Not Real')
    False
    """
    if magic_ident(item, 'ltd'): return False
    return any(magic_ident(item, sector) for sector in ['nhs', 'pub', 'odd'])


if __name__=='__main__':
    names = demofile()
    master = open(COMPANY_LIST, 'r').read().split('\n')
    print "%d rows"%len(names)
    nice=companies_wrapper(names, master)

