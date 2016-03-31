# API docs: http://apps.who.int/gho/athena/
# List of indicators: http://apps.who.int/gho/athena/data/GHO
import re
import requests
import messytables
import xypath
import dl
import collections
import orm
import logging

replacements = {'Display Value': 'value',
                'YEAR (DISPLAY)': 'period',
                'GHO (CODE)': 'indID',
                'COUNTRY (CODE)': 'region',
                'REGION (DISPLAY)': 'x_supregion',
                'GHO (DISPLAY)': 'indname'}

"""Value: dsID, region, indID, period, value, source, is_number
   DataSet: dsID, last_updated, last_scraped, name
   Indicator: indID, name, units
   """

dataset_template = {"dsID": "athena-api",
                    "last_updated": None,
                    "last_scraped": orm.now(),
                    "name": "WHO Athena API"}

orm.DataSet(**dataset_template).save()

indicators = list("CM_01,CM_02,CM_03,DEVICES09,DEVICES22,HIV_0000000020,MALARIA001,MDG_0000000001,MDG_0000000003,MDG_0000000005,MDG_0000000005_AGE1519,MDG_0000000006,MDG_0000000006_AGE1519,MDG_0000000007,MDG_0000000010,MDG_0000000011,MDG_0000000013,MDG_0000000014,MDG_0000000017,MDG_0000000020,MDG_0000000023,MDG_0000000025,MDG_0000000025_AGE1519,MDG_0000000026,MDG_0000000029,MDG_0000000031,MDG_0000000033,MDG_0000000034,MH_17,MH_18,MH_6,SA_0000001688,TB_1,TB_tot_newrel,TOBACCO_0000000192,WHOSIS_000001,WHOSIS_000003,WHOSIS_000005,WHOSIS_000011,WHOSIS_000015,WHS10_1,WHS10_8,WHS10_9,WHS2_138,WHS2_162,WHS2_163,WHS2_164,WHS2_165,WHS2_166,WHS2_167,WHS2_168,WHS2_170,WHS2_171,WHS2_172,WHS2_173,WHS2_174,WHS2_3070_all,WHS2_3070_cancer,WHS2_3070_cdd,WHS2_3070_chronic,WHS2_513,WHS2_514,WHS2_515,WHS2_516,WHS2_523,WHS3_40,WHS3_41,WHS3_42,WHS3_43,WHS3_45,WHS3_46,WHS3_47,WHS3_48,WHS3_49,WHS3_50,WHS3_51,WHS3_52,WHS3_53,WHS3_55,WHS3_56,WHS3_57,WHS3_62,WHS4_100,WHS4_106,WHS4_107,WHS4_108,WHS4_111,WHS4_111_AGE1519,WHS4_115,WHS4_117,WHS4_124,WHS4_128,WHS4_129,WHS4_154,WHS4_2530,WHS6_101,WHS6_102,WHS6_116,WHS6_123,WHS6_125,WHS6_127,WHS6_136,WHS6_140,WHS6_144,WHS6_148,WHS6_150,WHS6_517,WHS6_518,WHS6_519,WHS6_520,WHS7_103,WHS7_104,WHS7_105,WHS7_108,WHS7_113,WHS7_120,WHS7_134,WHS7_139,WHS7_143,WHS7_147,WHS7_149,WHS7_156,WHS8_110,WHS9_85,WHS9_86,WHS9_88,WHS9_89,WHS9_90,WHS9_91,WHS9_92,WHS9_93,WHS9_95,WHS9_96,WHS9_97,WHS9_CBR,WHS9_CDR,WHS9_CS,CM_01,CM_02,CM_03,DEVICES09,DEVICES22,HIV_0000000020,MALARIA001,MDG_0000000001,MDG_0000000003,MDG_0000000005,MDG_0000000005_AGE1519,MDG_0000000006,MDG_0000000006_AGE1519,MDG_0000000007,MDG_0000000010,MDG_0000000011,MDG_0000000013,MDG_0000000014,MDG_0000000017,MDG_0000000020,MDG_0000000023,MDG_0000000025,MDG_0000000025_AGE1519,MDG_0000000026,MDG_0000000029,MDG_0000000031,MDG_0000000033,MDG_0000000034,MH_17,MH_18,MH_6,SA_0000001688,TB_1,TB_tot_newrel,TOBACCO_0000000192,WHOSIS_000001,WHOSIS_000003,WHOSIS_000005,WHOSIS_000011,WHOSIS_000015,WHS10_1,WHS10_8,WHS10_9,WHS2_138,WHS2_162,WHS2_163,WHS2_164,WHS2_165,WHS2_166,WHS2_167,WHS2_168,WHS2_170,WHS2_171,WHS2_172,WHS2_173,WHS2_174,WHS2_3070_all,WHS2_3070_cancer,WHS2_3070_cdd,WHS2_3070_chronic,WHS2_513,WHS2_514,WHS2_515,WHS2_516,WHS2_523,WHS3_40,WHS3_41,WHS3_42,WHS3_43,WHS3_45,WHS3_46,WHS3_47,WHS3_48,WHS3_49,WHS3_50,WHS3_51,WHS3_52,WHS3_53,WHS3_55,WHS3_56,WHS3_57,WHS3_62,WHS4_100,WHS4_106,WHS4_107,WHS4_108,WHS4_111,WHS4_111_AGE1519,WHS4_115,WHS4_117,WHS4_124,WHS4_128,WHS4_129,WHS4_154,WHS4_2530,WHS6_101,WHS6_102,WHS6_116,WHS6_123,WHS6_125,WHS6_127,WHS6_136,WHS6_140,WHS6_144,WHS6_148,WHS6_150,WHS6_517,WHS6_518,WHS6_519,WHS6_520,WHS7_103,WHS7_104,WHS7_105,WHS7_108,WHS7_113,WHS7_120,WHS7_134,WHS7_139,WHS7_143,WHS7_147,WHS7_149,WHS7_156,WHS8_110,WHS9_85,WHS9_86,WHS9_88,WHS9_89,WHS9_90,WHS9_91,WHS9_92,WHS9_93,WHS9_95,WHS9_96,WHS9_97,WHS9_CBR,WHS9_CDR,WHS9_CS,WHS9_86,WHS9_88,WHS9_89,WHS9_92,WHS9_96,WHS9_97,WHS9_90,WHS3_48,MALARIA002,MALARIA001,MALARIA003,MDG_0000000013,MDG_0000000014,MENING_2,MENING_1,MENING_3,CHOLERA_0000000001,CHOLERA_0000000002,CHOLERA_0000000003,MDG_0000000027,WHOSIS_000009,NUTRITION_564,sba,sba3,sba5,anc1,anc13,MDG_0000000015,MDG_0000000021,anc4,anc43,anc45".split(","))

indicators = reversed(indicators)

baseurl = "http://apps.who.int/gho/athena/data/GHO/%s.csv?profile=verbose"

def units(s):
    # print repr(s)
    z = re.findall("(.*)\(([^)]*)\)$", s)
    if z:
        assert len(z) == 1
        return z[0]
    else:
        return (s, "")


def do_indicator(ind):
    print "indicator:", ind
    fh = dl.grab(baseurl % ind)
    mt, = messytables.commas.CSVTableSet(fh).tables
    mt_list = list(mt)
    try:
        headers = mt_list[0]
    except IndexError:
        headers = []
    if len(headers) == 0:
        print "Error getting headers from ", ind
        raise RuntimeError("No header in {}".format(ind) )
    logging.warn("headers {!r}".format(headers))
    rest = mt_list[1:]
    for row in rest:
        if len(row) == 0:
            continue  # skip empty row

        rowdict = {x[0].value: x[1].value for x in zip(headers, row)}
        try:
            name, unit = units(rowdict['GHO (DISPLAY)'])
        except Exception:
            fh.seek(0)
            print fh.read()
            raise
        indID = rowdict['GHO (CODE)']
        for lookup in ["SEX", "RESIDENCEAREATYPE", "EDUCATIONLEVEL", "WEALTHQUINTILE"]:
            lookup_code = lookup+" (CODE)"
            lookup_name = lookup+" (DISPLAY)"
            if lookup_code in rowdict:    # header = "SEX (CODE)"
                if rowdict[lookup_code]:  # value != ""
                    indID = indID + "({}={})" .format(lookup, rowdict[lookup_code])
                    name = name + " - " + rowdict[lookup_name]
        
        value_dict = {"value": rowdict['Display Value'],
                      "period": rowdict['YEAR (DISPLAY)'],
                      "indID": indID,
                      "region": rowdict["COUNTRY (CODE)"],
                      "dsID": "athena-api",
                      "source": baseurl % ind,
                      "is_number": True}
  
        
        indicator_dict = {'indID': indID,
                          'name': name,
                          'units': unit}

        orm.Indicator(**indicator_dict).save()
        orm.Value(**value_dict).save()
        # print value_dict
        
fail = 0
for ind in indicators:
    try:
        do_indicator(ind)
    except Exception as e:
        print e
        fail = 1
exit(fail)
    

# headers: Counter({u'REGION (DISPLAY)': 137, u'Comments': 137, u'Low': 137, u'YEAR (DISPLAY)': 137, u'YEAR (URL)': 137, u'GHO (DISPLAY)': 137, u'YEAR (CODE)': 137, u'PUBLISHSTATE (DISPLAY)': 137, u'GHO (CODE)': 137, u'Display Value': 137, u'REGION (URL)': 137, u'PUBLISHSTATE (URL)': 137, u'REGION (CODE)': 137, u'COUNTRY (DISPLAY)': 137, u'GHO (URL)': 137, u'PUBLISHSTATE (CODE)': 137, u'COUNTRY (URL)': 137, u'COUNTRY (CODE)': 137, u'Numeric': 137, u'High': 137, u'WORLDBANKINCOMEGROUP (DISPLAY)': 113, u'WORLDBANKINCOMEGROUP (CODE)': 113, u'WORLDBANKINCOMEGROUP (URL)': 113, u'SEX (CODE)': 9, u'SEX (DISPLAY)': 9, u'SEX (URL)': 9, u'RESIDENCEAREATYPE (DISPLAY)': 5, u'RESIDENCEAREATYPE (URL)': 5, u'RESIDENCEAREATYPE (CODE)': 5, u'DATASOURCE (DISPLAY)': 2, u'DATASOURCE (CODE)': 2, u'DATASOURCE (URL)': 2})

