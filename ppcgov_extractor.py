# -*- coding: utf-8 -*-
import os
import HTMLParser
import re
import datetime
import pprint

from BeautifulSoup import BeautifulSoup

import logging
FORMAT = "[%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format = FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

name_map = {u"機關代碼":     "entity_code",
            u"機關名稱":     "procuring_entity",
            u"單位名稱":     "unit",
            u"標案案號":     "job_number",
            u"招標方式":     "procurement_type",
            u"決標方式":     "tender_awarding_type",
            u"標案名稱":     "subject_of_procurement",
            u"決標資料類別": "attr_of_tender_awarding",
            u"標的分類":     "attr_of_procurement",
            u"預算金額":     "budget_value",
            u"開標時間":     "opening_date",
            u"決標公告日期": "tender_awarding_announce_date",
            u"歸屬計畫類別": "project_type",
            u"總決標金額":   "total_tender_awarding_value",
            u"底價金額":     "floor_price_value",
            u"決標日期":     "tender_awarding_date",
            u"底價金額是否公開":     "is_tender_public",
            "pkAtmMain":    "pkAtmMain"}

tender_award_item_map = {"得標廠商":    "awarded_tenderer",
                        "預估需求數量": "request_number",
                        "決標金額":     "tender_awarding_value",
                        "底價金額":     "floor_price_value"}

tenderer_map = {"廠商代碼":"tenderer_code", 
                "廠商名稱":"tenderer_name", 
                "是否得標":"awarded", 
                "組織型態":"orgnization_type"}

def load_all_tender_raw_detail_info():
    folder_name = 'tender_raw_detail_info'
    files = [f for f in os.listdir(folder_name)]

    for detail_info_file_path in files:
        with open(os.path.join(folder_name, detail_info_file_path), 'r') as detail_info_file:
            content = detail_info_file.read()

        yield content

def parse_tender_detail_info(info_soup):
    award_info_dic = __get_award_info_dic(info_soup)
    
    tender_table = info_soup.find('table', { "class" : "table_block tender_table" }) 
    tr = tender_table.findAll('tr') 

    tenderer_info_dic = __get_tenderer_info_dic(tr) 
    tender_award_item_dic = __get_tender_award_item_dic(tr)

    #pprint.pprint(award_info_dic)
    #pprint.pprint(tender_award_item_dic)
    #pprint.pprint(tenderer_info_dic)

def __get_award_info_dic(info_soup):
    #機關資料
    needed_keys = [u"機關代碼", u"機關名稱", u"單位名稱"]
    award_dic = __basic_get_award_info_dic(info_soup, needed_keys, 1)

    #已公告資料
    needed_keys =  [u"標案案號", 
                    u"招標方式", 
                    u"決標方式", 
                    u"標案名稱", 
                    u"決標資料類別", 
                    u"標的分類", 
                    u"開標時間",
                    u"預算金額",
                    u"歸屬計畫類別",
                ]
    public_dic = __basic_get_award_info_dic(info_soup, needed_keys, 2)

    #決標資料
    needed_keys = [ u"決標日期", 
                    u"底價金額", 
                    u"總決標金額", 
                    u"決標公告日期", 
                    u"底價金額是否公開"
                ]
    tender_result_dic = __basic_get_award_info_dic(info_soup, needed_keys, 6)

    return dict(award_dic.items() + public_dic.items() + tender_result_dic.items())

def __basic_get_award_info_dic(info_soup, needed_keys, table_id):
    info_dic = __transfer_award_table_to_dic(info_soup, table_id)

    filtered_dic = {}

    for needed_key in needed_keys:
        if info_dic.has_key(needed_key):
            filtered_dic[name_map[needed_key]] = __transfer_data_format(info_dic[needed_key])
    
    return filtered_dic

def __get_tender_award_item_dic(element):
    returned_dic = {}
    item_num = 0
    grp_num = 0
    for tr in element:
        if tr.get('class') == 'award_table_tr_4' and tr.find('table') is not None:
            row = tr.find('table').findAll('tr')
            for r in row:
                if r.find('th') is not None:
                    th = r.find('th').text.encode('utf-8').strip()
                    m = re.match(r'第(\d+)品項' ,th)
                    m2 = re.match(r'得標廠商(\d+)' ,th)
                    if m is not None:
                        item_num =  int(m.group(1).decode('utf-8'))
                        returned_dic[item_num] = {}
                    elif m2 is not None:
                        grp_num =  int(m2.group(1).decode('utf-8'))
                        returned_dic[item_num][grp_num] = {}                    
                    else:
                        if th in tender_award_item_map:
                            if th == "決標金額" or th == "底價金額":
                                returned_dic[item_num][grp_num][tender_award_item_map[th]] = __transfer_data_format(r.find('td').text)                                                        
                            else:
                                returned_dic[item_num][grp_num][tender_award_item_map[th]] = r.find('td').text
    return returned_dic

def __transfer_data_format(data):
    data = __remove_escape_and_space(data)
    date_pattern = r"(?P<Y>[0-9]{3})/(?P<m>[0-9]{2})/(?P<d>[0-9]{2})"
    time_pattern = r"(?P<H>[0-9]{2}):(?P<M>[0-9]{2}"
    cost_pattern = r"\$?-?([0-9,]+)"

    #Date
    m = re.search(date_pattern, data)
    if m != None:
        year = int(m.group("Y")) + 1911
        month = int(m.group("m"))
        day = int(m.group("d"))
        return datetime.date(year, month, day)

    #Cost
    m = re.search(cost_pattern, data)
    if m != None:
        return ''.join(m.group(1).split(','))    

    return data

def __get_tenderer_info_dic (element):
    #投標廠商
    returned_dic = {}
    grp_num = 0
    for tr in element:
        if  tr.get('class') == 'award_table_tr_3' and tr.find('table') is not None:
            row = tr.find('table').findAll('tr')
            for r in row:
                if r.find('th') is not None:
                    th = r.find('th').text.encode('utf-8').strip()
                    m = re.match(r'投標廠商(\d+)' ,th)
                    if m is not None:
                        grp_num =  int(m.group(1).decode('utf-8'))
                        returned_dic[grp_num] = {'tenderer_num': grp_num}
                    else:
                        if th in tenderer_map:
                            returned_dic[grp_num][tenderer_map[th]] = r.find('td').text
    return returned_dic 

def __remove_escape_and_space(text):
    return HTMLParser.HTMLParser().unescape("".join(text.split()))

def __transfer_award_table_to_dic(info_soup, table_id):
    table_id = "award_table_tr_%d" % table_id

    dic = {}

    #Because the first tr is empty, skip it
    table_items = info_soup.findAll("tr", {"class" : table_id})[1:]
    for table_item in table_items:
        if table_item.th != None and table_item.td != None:
            dic[table_item.th.text] = table_item.td.text

    return dic

if __name__ == "__main__":
    raw_detail_info_iter = load_all_tender_raw_detail_info()

    for raw_detail_info in raw_detail_info_iter:
        info_soup = BeautifulSoup(''.join(raw_detail_info))
        parse_tender_detail_info(info_soup)

    print "end"

