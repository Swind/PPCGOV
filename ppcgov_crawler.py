# -*- coding: utf-8 -*-
import requests
import re
from math import ceil 
from BeautifulSoup import BeautifulSoup

import logging
FORMAT = "[%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format = FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

from ppcdef import ORG_IDS
from ppcdef import PAYLOAD 

def query_tender_links(org_id, start_date, end_date):
    PAYLOAD['awardAnnounceStartDate'] = start_date
    PAYLOAD['awardAnnounceEndDate'] = end_date 
    PAYLOAD['orgId'] = org_id

    logger.info("Get data of %s from %s to %s" % (org_id, start_date, end_date))

    #Get all tender links
    return __get_tender_links(PAYLOAD)

def __get_tender_links(post_data):

    rs = requests.session() 
    rs_post = rs.post("http://web.pcc.gov.tw/tps/pss/tender.do?searchMode=common&searchType=advance", data = post_data)     

    max_page_number = __get_max_page_number(rs_post)
    logger.info("There are %d page date" % max_page_number)

    return __get_tender_links_in_every_page(rs, max_page_number)

def __get_max_page_number(resp):
    response_text = resp.text.encode('utf8') 

    soup = BeautifulSoup(''.join(response_text)) 
    rec_number_element = soup.find('span', { "class" : "T11b" }) 
    rec_number = int(rec_number_element.text)

    logger.info("There are %d tender records" % rec_number)

    page_number = int(ceil(float(rec_number) / 100)) 

    return page_number

def __get_tender_links_in_every_page(session, max_page_number):
    page_format = "http://web.pcc.gov.tw/tps/pss/tender.do?searchMode=common&searchType=advance&searchTarget=ATM&method=search&isSpdt=&pageIndex=%d" 

    tender_links = []
    
    for page in range(1, max_page_number + 1): 
        bid_list = session.get(page_format%(page)) 
        bid_response = bid_list.text.encode('utf8')

        bid_soup = BeautifulSoup(''.join(bid_response)) 
        bid_table = bid_soup.find('div', { "id" : "print_area" }) 

        bid_rows = bid_table.findAll('tr')[1:-1] 
        
        for bid_row in bid_rows: 
            link = [tag.attrMap['href'] 
              for tag in bid_row.findAll('a',{'href': True})][0] 

            link_href = "http://web.pcc.gov.tw/tps" + link[2:] 
            tender_links.append(link_href)

    return tender_links

def get_all_org_ids():
    raw = requests.get(
        "http://web.pcc.gov.tw/tps/main/pss/pblm/tender/basic/search/mainListCommon.jsp").text.encode('utf8')
    raw_soup = BeautifulSoup(raw)
    
    #Find all <a> with sytle "color: blue;"
    org_ids = {} 

    id_pattern = re.compile(r'([\W]+)\(([0-9\.]+)\)')
    for raw_id_tag in raw_soup.findAll('a', style="color: blue;"):
        key_value = re.search(id_pattern, raw_id_tag.getText())

        if key_value != None:
            org_ids[key_value.group(1)] = key_value.group(2)

    return org_ids

def save_tender_links(file_name, links):
    bid_file = open(file_name, 'w')
    
    logger.info("Save links in %s" % file_name)

    for link in links:
        bid_file.write(link + "\n")

    bid_file.close()

    logger.info("Save %d links in %s successed" % (len(links), file_name))

def load_tender_links(file_name):
    links = []

    logger.info("Load links from %s" % file_name)

    with open(file_name, 'r') as bid_file:
        for link in bid_file.readlines():
            links.append(link)

    logger.info("Load %d links from %s successed" % (len(links), file_name))
    
    return links

def get_tender_raw_detail_info(tender_link):
    resp = requests.get(tender_link).text.encode('utf-8') 

    soup = BeautifulSoup(''.join(resp))
    printarea = soup.find('div', id = "printArea")

    return printarea.prettify("utf-8")


def get_tender_key_from_link(tender_link):
    #params = tender_link.strip().split("pkAtmMain=")[1].split("&tenderCaseNo=")
    #return (params[0], params[1])

    pattern = r"([^ ]+)pkAtmMain=(?P<pkAtmMain>\w+)&tenderCaseNo=(?P<tenderCaseNo>[\w-]+)"
    m = re.match(pattern, tender_link)
    return (m.group('pkAtmMain'), m.group('tenderCaseNo'))

def save_all_tender_raw_detail_info(links):
    for link in links:
        raw_info = get_tender_raw_detail_info(link)
        tender_keys = get_tender_key_from_link(link)

        __save_tender_raw_detail_info(raw_info, tender_keys)

def __save_tender_raw_detail_info(raw_info, tender_keys):
    with open('tender_raw_detail_info/%s_%s.txt' % tender_keys, 'w') as info_file:
       info_file.write(raw_info) 


if __name__ == "__main__":
    links = query_tender_links(ORG_IDS[u'國防部'], "103/03/01", "103/05/10")
    save_tender_links("bid_list.txt", links)

    links = load_tender_links("bid_list.txt")
    save_all_tender_raw_detail_info(links)

