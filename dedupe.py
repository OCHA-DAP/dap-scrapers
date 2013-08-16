import collections
import re
from collections import defaultdict, Counter

def get_one(it):
    """return first item from iterator
    >>> get_one([1,2,3])
    1
    >>> get_one(set([1,2,3])) in set([1,2,3])
    True
    """
    for i in it:
        return i

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
    'cat  not really '
    """
    return re.sub('[^\w ]',' ',i,flags=re.UNICODE)

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

def purge_prefix_zero(i):
    """
    >>> purge_prefix_zero("007")
    '7'
    >>> purge_prefix_zero("0800")
    '800'
    """
    return re.sub('^0*','', i.strip())

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

def merge_space(i):
    """
    >>> merge_space(u"\xa0a \xa0b ")
    u'a b'
    """
    return re.sub('\s+', ' ', i, flags=re.UNICODE).strip()

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

functionlist = [identity, ampersand, lcase, purgebracket, nopunct, merge_space, purge_prefix_zero]
shortlist = [identity, ampersand, lcase, nopunct, merge_space, purge_prefix_zero]
difflist=[]
keepkey = []

def apply_one_keep_bracket(name):
    return apply_one(name, flist=shortlist)

def apply_one(name, flist=None):
    if not flist:
        flist = functionlist
    for f in flist:
        name = f(name)
    return name
