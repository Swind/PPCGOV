import sqlite3

import ppcgov_extractor
from BeautifulSoup import BeautifulSoup

import logging
FORMAT = "[%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format = FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database(db_name):
    conn = sqlite3.connect(db_name)    

    with conn:
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS Tender_awards(
                id INTEGER PRIMARY KEY,
                pkAtmMain TEXT,
                procuring_entity TEXT,
                entity_code TEXT,
                attr_of_procurement TEXT,
                opening_date DATETIME,
                procurement_type TEXT,
                tender_awarding_type TEXT,
                project_type TEXT,
                subject_of_procurement TEXT,
                job_number TEXT,
                budget_value BIGINTEGER,
                attr_of_tender_awarding TEXT,
                floor_price_value BIGINTEGER,
                tender_awarding_announce_date DATETIME,
                tender_awarding_date DATETIME,
                total_tender_awarding_value BIGINTEGER,
                is_tender_public TEXT
                )''') 

        cursor.execute('''CREATE TABLE IF NOT EXISTS Tenderer(
                    id INTEGER PRIMARY KEY,
                    pkAtmMain TEXT,
                    job_number TEXT,
                    tenderer_code TEXT,
                    tenderer_name TEXT,                
                    awarded TEXT,
                    orgnization_type TEXT,
                    tenderer_num INT
                    )''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS Tender_award_item(
                    id INTEGER PRIMARY KEY,
                    pkAtmMain TEXT,
                    job_number TEXT,
                    awarded_tenderer TEXT,
                    request_number INT,                
                    tender_awarding_value BIGINT,
                    floor_price_value BIGINT,
                    item_num INT,
                    awarded_num INT
                    )''') 
    conn.close()

def insert_award_info(cur, pkAtmMain, job_number, dic):
    dic['pkAtmMain'] = pkAtmMain
    columns = ", ".join(dic.keys())
    placeholders = ", ".join('?' * len(dic))
    sql = "INSERT INTO Tender_awards ({}) VALUES ({})".format(columns, placeholders)
    
    cur.execute(sql, dic.values())


def insert_tenderer_info (cur, pkAtmMain, job_number, data_dic):
    tenderer_sql = 'INSERT INTO Tenderer (pkAtmMain, job_number, tenderer_code, tenderer_name, awarded, orgnization_type, tenderer_num) VALUES (?,?,?,?,?,?,?)'
    for ele in data_dic:                        
        cur.execute(tenderer_sql, (pkAtmMain, job_number,  data_dic[ele]["tenderer_code"], data_dic[ele]["tenderer_name"],
          data_dic[ele]["awarded"], data_dic[ele]["orgnization_type"], ele))
    
def insert_tender_award_item_info(cur, pkAtmMain, job_number, data_dic):
    tenderawarditem_sql = 'INSERT INTO Tender_award_item (pkAtmMain, job_number, awarded_tenderer, request_number, tender_awarding_value, floor_price_value, item_num, awarded_num) VALUES (?,?,?,?,?,?,?,?)' 

    for item in data_dic:
        for grp in data_dic[item]:
            if 'floor_price_value' not in data_dic[item][grp]:
                data_dic[item][grp]['floor_price_value'] = None
            if 'tender_awarding_value' not in data_dic[item][grp]:
                data_dic[item][grp]['tender_awarding_value'] = None
            cur.execute(tenderawarditem_sql, (pkAtmMain, job_number,  data_dic[item][grp]["awarded_tenderer"],  data_dic[item][grp]["request_number"],
                   data_dic[item][grp]["tender_awarding_value"],  data_dic[item][grp]["floor_price_value"], item, grp)) 

if __name__ == "__main__":
    init_database("tender.db")
    db = sqlite3.connect('tender.db') 
    cur = db.cursor()   

    raw_detail_info_iter = ppcgov_extractor.load_all_tender_raw_detail_info()
    
    for pkAtmMain, job_number, raw_detail_info in raw_detail_info_iter:
        info_soup = BeautifulSoup(''.join(raw_detail_info))
        award_info_dic, tenderer_info_dic, tender_award_item_dic = ppcgov_extractor.parse_tender_detail_info(info_soup)
    
        logger.info("Insert %s-%s to database" % (pkAtmMain, job_number)) 
        
        insert_award_info(cur, pkAtmMain, job_number, award_info_dic)
        insert_tenderer_info(cur, pkAtmMain, job_number, tenderer_info_dic)
        insert_tender_award_item_info(cur, pkAtmMain, job_number, tender_award_item_dic)

    db.commit()
    db.close()
