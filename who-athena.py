import StringIO
import requests
import xypath
import messytables
from messytables import headers_processor
from hamcrest import equal_to, is_in
from orm import session, Value, DataSet, Indicator, send
import orm
import dl
import re
import lxml
import requests_cache

requests_cache.install_cache("who_cache.db")

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

baseurl = "http://apps.who.int/gho/athena/data/GHO/CM_01,CM_02,CM_03,DEVICES09,DEVICES22,HIV_0000000020,MALARIA001,MDG_0000000001,MDG_0000000003,MDG_0000000005,MDG_0000000005_AGE1519,MDG_0000000006,MDG_0000000006_AGE1519,MDG_0000000007,MDG_0000000010,MDG_0000000011,MDG_0000000013,MDG_0000000014,MDG_0000000017,MDG_0000000020,MDG_0000000023,MDG_0000000025,MDG_0000000025_AGE1519,MDG_0000000026,MDG_0000000029,MDG_0000000031,MDG_0000000033,MDG_0000000034,MH_17,MH_18,MH_6,SA_0000001688,TB_1,TB_tot_newrel,TOBACCO_0000000192,WHOSIS_000001,WHOSIS_000003,WHOSIS_000005,WHOSIS_000011,WHOSIS_000015,WHS10_1,WHS10_8,WHS10_9,WHS2_138,WHS2_162,WHS2_163,WHS2_164,WHS2_165,WHS2_166,WHS2_167,WHS2_168,WHS2_170,WHS2_171,WHS2_172,WHS2_173,WHS2_174,WHS2_3070_all,WHS2_3070_cancer,WHS2_3070_cdd,WHS2_3070_chronic,WHS2_513,WHS2_514,WHS2_515,WHS2_516,WHS2_523,WHS3_40,WHS3_41,WHS3_42,WHS3_43,WHS3_45,WHS3_46,WHS3_47,WHS3_48,WHS3_49,WHS3_50,WHS3_51,WHS3_52,WHS3_53,WHS3_55,WHS3_56,WHS3_57,WHS3_62,WHS4_100,WHS4_106,WHS4_107,WHS4_108,WHS4_111,WHS4_111_AGE1519,WHS4_115,WHS4_117,WHS4_124,WHS4_128,WHS4_129,WHS4_154,WHS4_2530,WHS6_101,WHS6_102,WHS6_116,WHS6_123,WHS6_125,WHS6_127,WHS6_136,WHS6_140,WHS6_144,WHS6_148,WHS6_150,WHS6_517,WHS6_518,WHS6_519,WHS6_520,WHS7_103,WHS7_104,WHS7_105,WHS7_108,WHS7_113,WHS7_120,WHS7_134,WHS7_139,WHS7_143,WHS7_147,WHS7_149,WHS7_156,WHS8_110,WHS9_85,WHS9_86,WHS9_88,WHS9_89,WHS9_90,WHS9_91,WHS9_92,WHS9_93,WHS9_95,WHS9_96,WHS9_97,WHS9_CBR,WHS9_CDR,WHS9_CS.html?profile=ztable&filter=COUNTRY:%s;SEX:-;LOCATION:-;WORLDBANKINCOMEGROUP:-;DATASOURCE:-;RESIDENCEAREATYPE:-;WEALTHQUINTILE:-;YEAR:2013,YEAR:2012;YEAR:2011;YEAR:2010;YEAR:2009;YEAR:2008;YEAR:2007;YEAR:2006;YEAR:2005;YEAR:2004;YEAR:2003;YEAR:2002"

secondurl = "http://apps.who.int/gho/athena/data/data-verbose.csv?target=GHO/CM_01,CM_02,CM_03,DEVICES09,DEVICES22,HIV_0000000020,MALARIA001,MDG_0000000001,MDG_0000000003,MDG_0000000005,MDG_0000000005_AGE1519,MDG_0000000006,MDG_0000000006_AGE1519,MDG_0000000007,MDG_0000000010,MDG_0000000011,MDG_0000000013,MDG_0000000014,MDG_0000000017,MDG_0000000020,MDG_0000000023,MDG_0000000025,MDG_0000000025_AGE1519,MDG_0000000026,MDG_0000000029,MDG_0000000031,MDG_0000000033,MDG_0000000034,MH_17,MH_18,MH_6,SA_0000001688,TB_1,TB_tot_newrel,TOBACCO_0000000192,WHOSIS_000001,WHOSIS_000003,WHOSIS_000005,WHOSIS_000011,WHOSIS_000015,WHS10_1,WHS10_8,WHS10_9,WHS2_138,WHS2_162,WHS2_163,WHS2_164,WHS2_165,WHS2_166,WHS2_167,WHS2_168,WHS2_170,WHS2_171,WHS2_172,WHS2_173,WHS2_174,WHS2_3070_all,WHS2_3070_cancer,WHS2_3070_cdd,WHS2_3070_chronic,WHS2_513,WHS2_514,WHS2_515,WHS2_516,WHS2_523,WHS3_40,WHS3_41,WHS3_42,WHS3_43,WHS3_45,WHS3_46,WHS3_47,WHS3_48,WHS3_49,WHS3_50,WHS3_51,WHS3_52,WHS3_53,WHS3_55,WHS3_56,WHS3_57,WHS3_62,WHS4_100,WHS4_106,WHS4_107,WHS4_108,WHS4_111,WHS4_111_AGE1519,WHS4_115,WHS4_117,WHS4_124,WHS4_128,WHS4_129,WHS4_154,WHS4_2530,WHS6_101,WHS6_102,WHS6_116,WHS6_123,WHS6_125,WHS6_127,WHS6_136,WHS6_140,WHS6_144,WHS6_148,WHS6_150,WHS6_517,WHS6_518,WHS6_519,WHS6_520,WHS7_103,WHS7_104,WHS7_105,WHS7_108,WHS7_113,WHS7_120,WHS7_134,WHS7_139,WHS7_143,WHS7_147,WHS7_149,WHS7_156,WHS8_110,WHS9_85,WHS9_86,WHS9_88,WHS9_89,WHS9_90,WHS9_91,WHS9_92,WHS9_93,WHS9_95,WHS9_96,WHS9_97,WHS9_CBR,WHS9_CDR,WHS9_CS&profile=verbose&filter=COUNTRY:ALL;SEX:-;LOCATION:-;WORLDBANKINCOMEGROUP:-;DATASOURCE:-;RESIDENCEAREATYPE:-;WEALTHQUINTILE:-;YEAR:2013;YEAR:2012;YEAR:2011;YEAR:2010;YEAR:2009;YEAR:2008;YEAR:2007;YEAR:2006;YEAR:2005;YEAR:2004;YEAR:2003;YEAR:2002"

dataset = {'dsID':'who-athena',
           'last_updated':None,
           'last_scraped':orm.now(),
           'name': 'World Health Organization - Athena'}

DataSet(**dataset).save()    



def getcountrylist():
    for value in session.query(Value).filter(Value.indID == "m49-name").all():
        yield value.region

def getcountry(threeletter="PAK"):
    print threeletter
    value = {'dsID':'who-athena',
             'source':baseurl % threeletter,
             'is_number':True}
    
    fh = dl.grab(baseurl%threeletter, timeout=20)
    messy = messytables.HTMLTableSet(fh)
    m_table = messy.tables[0]
    table = xypath.Table.from_messy(m_table)
    
    def cell_links(b):
        html = b.properties['html']
        root = lxml.html.fromstring(html)
        return root.xpath('.//a')

    rows = table.rows() # table has no rows?
    headers = {
               'value': table.filter("DISPLAY VALUE").x,
               'period': table.filter("YEAR").x,
               'indID': table.filter("GHO").x,
               'region': table.filter("COUNTRY").x,
               'x-supregion': table.filter("REGION").x,
              }

    def at_header(h):
        return lambda b: b.x == headers[h]

    print headers
    counter = 0
    for row in rows:
        if row.filter(at_header('indID')).value=='GHO':
            assert row.filter('GHO').y==0, row.filter('GHO').y
            continue
        
        vdict = dict(value)
        for h in headers:
            vdict[h] = row.filter(at_header(h)).value
        vdict['value']=vdict['value'].partition('[')[0].strip()
        if not vdict['region']:
            print repr(vdict['region']), repr(vdict['x-supregion'])
            vdict['region']=vdict['x-supregion']
        del vdict['x-supregion']

        indicator = {'indID':vdict['indID'],
                     'units':None,
                     'name':vdict['indID']}
        Indicator(**indicator).save()
        v = Value(**vdict)
        if not v.is_blank(): 
            v.save()
        counter=counter+1
    print counter
    session.commit()

getcountry("ALL")
