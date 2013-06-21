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
indicator_list = """
SP.POP.TOTL
...
SI.POV.GINI""".strip().split('\n')


"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

baseurl = "http://apps.who.int/gho/athena/data/GHO/CM_01,CM_02,CM_03,DEVICES09,DEVICES22,HIV_0000000020,MALARIA001,MDG_0000000001,MDG_0000000003,MDG_0000000005,MDG_0000000005_AGE1519,MDG_0000000006,MDG_0000000006_AGE1519,MDG_0000000007,MDG_0000000010,MDG_0000000011,MDG_0000000013,MDG_0000000014,MDG_0000000017,MDG_0000000020,MDG_0000000023,MDG_0000000025,MDG_0000000025_AGE1519,MDG_0000000026,MDG_0000000029,MDG_0000000031,MDG_0000000033,MDG_0000000034,MH_17,MH_18,MH_6,SA_0000001688,TB_1,TB_tot_newrel,TOBACCO_0000000192,WHOSIS_000001,WHOSIS_000003,WHOSIS_000005,WHOSIS_000011,WHOSIS_000015,WHS10_1,WHS10_8,WHS10_9,WHS2_138,WHS2_162,WHS2_163,WHS2_164,WHS2_165,WHS2_166,WHS2_167,WHS2_168,WHS2_170,WHS2_171,WHS2_172,WHS2_173,WHS2_174,WHS2_3070_all,WHS2_3070_cancer,WHS2_3070_cdd,WHS2_3070_chronic,WHS2_513,WHS2_514,WHS2_515,WHS2_516,WHS2_523,WHS3_40,WHS3_41,WHS3_42,WHS3_43,WHS3_45,WHS3_46,WHS3_47,WHS3_48,WHS3_49,WHS3_50,WHS3_51,WHS3_52,WHS3_53,WHS3_55,WHS3_56,WHS3_57,WHS3_62,WHS4_100,WHS4_106,WHS4_107,WHS4_108,WHS4_111,WHS4_111_AGE1519,WHS4_115,WHS4_117,WHS4_124,WHS4_128,WHS4_129,WHS4_154,WHS4_2530,WHS6_101,WHS6_102,WHS6_116,WHS6_123,WHS6_125,WHS6_127,WHS6_136,WHS6_140,WHS6_144,WHS6_148,WHS6_150,WHS6_517,WHS6_518,WHS6_519,WHS6_520,WHS7_103,WHS7_104,WHS7_105,WHS7_108,WHS7_113,WHS7_120,WHS7_134,WHS7_139,WHS7_143,WHS7_147,WHS7_149,WHS7_156,WHS8_110,WHS9_85,WHS9_86,WHS9_88,WHS9_89,WHS9_90,WHS9_91,WHS9_92,WHS9_93,WHS9_95,WHS9_96,WHS9_97,WHS9_CBR,WHS9_CDR,WHS9_CS.html?profile=ztable&filter=COUNTRY:%s;SEX:-;LOCATION:-;WORLDBANKINCOMEGROUP:-;DATASOURCE:-;RESIDENCEAREATYPE:-;WEALTHQUINTILE:-;YEAR:2013,YEAR:2012;YEAR:2011;YEAR:2010;YEAR:2009;YEAR:2008;YEAR:2007;YEAR:2006;YEAR:2005;YEAR:2004;YEAR:2003;YEAR:2002"


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
             'region':threeletter,
             'source':baseurl % threeletter,
             'is_number':True}
    
    fh = dl.grab(baseurl%threeletter)
    messy = messytables.HTMLTableSet(fh)
    m_table = messy.tables[0]
    offset, header = messytables.headers.headers_guess(m_table)
    print header
    m_table.register_processor(headers_processor(header))
    table = xypath.Table.from_messy(m_table)
    
    def cell_links(b):
        html = b.properties['html']
        root = lxml.html.fromstring(html)
        return root.xpath('.//a')

    rows = table.filter(cell_links)
    for row in rows:
        print row.y

    for indicator in indicators:
        vdict = dict(value)
        vdict['indID'] = ind_cell.value
        vdict['period'] = junction.value
        vdict['value'] = value_cell.value

        indicator = {'indID':vdict['indID']}
        nameunits = re.search('(.*)\((.*)\)',vdict['indID'])
        if nameunits:
            (indicator['name'], indicator['units'])=nameunits.groups()
        else:
            indicator['name']=vdict['indID']
            indicator['units']='uno'
        Indicator(**indicator).save()
        v = Value(**vdict)
        if not v.is_blank: 
            v.save()
    session.commit()

for country in getcountrylist():
    getcountry(country)
    exit()
