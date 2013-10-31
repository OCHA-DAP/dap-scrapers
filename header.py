import xypath
from itertools import groupby

def headerheader(self, dir1, dir2, **kwargs):  # XYZZY
    def group(self, keyfunc=None):  # XYZZY
        """get a dictionary containing lists of singleton bags with the same
           value (by default; other functions available)"""
        groups = {}
        if keyfunc is None:
            keyfunc = lambda x: x.value
        protogroups = groupby(sorted(self, key=keyfunc), key=keyfunc)
        for k, v in protogroups:
            newbag = xypath.Bag.from_list(v)
            newbag.table = self.table
            groups[k] = newbag
        return groups
    """Given a header (e.g. "COUNTRY") get all things in one direction
       from it (e.g. down: "FRANCE", "GERMANY"), then use those to get
       a suitable xyzzy dict"""
    header = group(self.fill(dir1), **kwargs)
    return {k: header[k].fill(dir2) for k in header}

