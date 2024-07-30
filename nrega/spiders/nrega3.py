import scrapy,requests
import json
import re
import os
import pandas as pd
import warnings
from scrapy.selector import Selector
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import logging

class MySpider(scrapy.Spider):
    name = 'nrega1'
    start_urls = ['https://nregastrep.nic.in']  # This will be populated later

  


    def __init__(self, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.asp_net_session_id = self.get_new_cookies()
        self.count = 0
        self.load_links_from_excel()
        self.json_filename = 'combined_data3.json'
        self.all_data = self.load_existing_data(self.json_filename)
        self.not_scraped_filename = 'not_scraped.json'
        self.not_scraped_data = self.load_existing_data(self.not_scraped_filename)

    def load_links_from_excel(self):
        df = pd.read_excel('MUZAFFARPUR.xlsx')
        self.start_urls = df['link'].tolist()[20000:30000]

    def get_new_cookies(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        asp_net_session_id = None
        try:
            driver.get('https://nregastrep.nic.in/netnrega/loginframegp.aspx?lflag=eng&page=C&state_code=05&Digest=WTBLudWe5FkJHTnLLorVEQ')
            time.sleep(2)

            def select_option(xpath):
                element = driver.find_element(By.XPATH, xpath)
                if element:
                    element.click()
                    time.sleep(1.5)

            select_option('//*[@name="ctl00$ContentPlaceHolder1$ddlFin"]/option[2]')
            select_option('//*[@name="ctl00$ContentPlaceHolder1$ddlDistrict"]/option[2]')
            select_option('//*[@name="ctl00$ContentPlaceHolder1$ddlBlock"]/option[2]')
            select_option('//*[@name="ctl00$ContentPlaceHolder1$ddlPanchayat"]/option[2]')
            select_option('//*[@value="Proceed"]')

            cookies = driver.get_cookies()
            for cookie in cookies:
                if cookie['name'] == 'ASP.NET_SessionId':
                    asp_net_session_id = cookie['value']
                    print(asp_net_session_id)
                    break
        except Exception as e:
            pass
            #logging.error(f"Error fetching cookies: {e}")
        finally:
            driver.quit()
        return asp_net_session_id

    def fetch_url(self, url, headers):
        retries = 0
        max_retries = 10
        while retries < max_retries:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    return response
                elif response.status_code == 503:
                    logging.warning("Service Unavailable. Waiting for 10 minutes before retrying...")
                    print("Service Unavailable. Waiting for 10 minutes before retrying...")
                    time.sleep(600)
                    retries += 1
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"Request failed: {e}")
                retries += 1
        return None

    def safe_extract_first(self, selector):
        extracted = selector.extract_first()
        return extracted.strip() if extracted else None

    def save_to_json(self, filename, data):
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    def load_existing_data(self, filename):
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as file:
                return json.load(file)
        return []

    def parse(self, response):
        url = response.url
        #time.sleep(0.1)
        if self.count % 500 == 0 and self.count != 0:
            self.asp_net_session_id = self.get_new_cookies()

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': f'ASP.NET_SessionId={self.asp_net_session_id}',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

        data = self.fetch_url(url, headers)

        attempts = 0
        while data and "File is under process, please wait for some time" in data.text:
            logging.info("File is under process. Waiting for 1 minute before retrying...")
            time.sleep(30)
            self.asp_net_session_id = self.get_new_cookies()
            headers['Cookie'] = f'ASP.NET_SessionId={self.asp_net_session_id}'
            data = self.fetch_url(url, headers)
            attempts += 1
            if attempts >= 20:
                logging.error(f"Failed to retrieve {url} after multiple attempts.")
                self.not_scraped_data.append({'url': url, 'reason': 'Failed to retrieve after multiple attempts'})
                break

        self.count += 1
        response = Selector(text=data.text)
        try:
            job_card_no = self.safe_extract_first(response.xpath('//*[contains(text(),"Job card No.:")]/following::td[1]/font/b/text()'))
            if job_card_no:
                print(job_card_no)
            else:
                print("None")
            name_head_household = self.safe_extract_first(response.xpath('//*[@id="lbl_house"]/text()'))
            name_father_or_husband = self.safe_extract_first(response.xpath('//*[@id="lbl_FATH_HUS"]/text()'))
            category = self.safe_extract_first(response.xpath('//*[contains(text(),"Category:")]/following::td[1]/b/font/text()'))
            registration_date = self.safe_extract_first(response.xpath('//*[@id="lbl_head"]/text()'))
            address = self.safe_extract_first(response.xpath('//*[@id="lbl_add"]/text()'))
            village = self.safe_extract_first(response.xpath('//*[@id="lbl_vill"]/text()'))
            panchayat = self.safe_extract_first(response.xpath('//*[contains(text(),"Panchayat:")]/following::td[1]/font/b/text()'))
            block = self.safe_extract_first(response.xpath('//*[contains(text(),"Block:")]/following::td[1]/font/b/text()'))
            district = self.safe_extract_first(response.xpath('//*[contains(text(),"District:")]/following::td[1]/font/b/text()'))
            BPL_yn = self.safe_extract_first(response.xpath('//*[contains(text(),"District:")]/following::td[3]/b/font/text()'))
            family_id = self.safe_extract_first(response.xpath('//*[@id="lbl_famid"]/text()'))

            url_data = {
                'url': url,
                'job_card': {
                    'job_card_no': job_card_no,
                    'name_head_household': name_head_household,
                    'name_father_or_husband': name_father_or_husband,
                    'category': category,
                    'registration_date': registration_date,
                    'address': address,
                    'village': village,
                    'panchayat': panchayat,
                    'block': block,
                    'district': district,
                    'BPL_yn': BPL_yn,
                    'family_id': family_id,
                },
                'applicant_details': [],
                'employment_requested': [],
                'employment_offered': [],
                'employment_given': []
            }

            name_rows = response.xpath('//*[@id="GridView4"]//tr')
            if len(name_rows) > 0:
                for row in range(4, len(name_rows) + 1, 2):
                    name = self.safe_extract_first(response.xpath(f'//*[@id="GridView4"]//tr[{row}]/td[2]/text()'))
                    gender = self.safe_extract_first(response.xpath(f'//*[@id="GridView4"]//tr[{row}]/td[3]/text()'))
                    age = self.safe_extract_first(response.xpath(f'//*[@id="GridView4"]//tr[{row}]/td[4]/text()'))
                    bank_po = self.safe_extract_first(response.xpath(f'//*[@id="GridView4"]//tr[{row}]/td[5]/text()'))
                    url_data['applicant_details'].append({
                        'name': name,
                        'gender': gender,
                        'age': age,
                        'bank_po': bank_po
                    })

            name_rows = response.xpath('//*[@id="GridView1"]//tr')

            if len(name_rows) > 0:
                for row in range(4, len(name_rows) + 1,2):
                    sno = self.safe_extract_first(response.xpath(f'//*[@id="GridView1"]//tr[{row}]/td[1]/text()'))
                    demand_id = self.safe_extract_first(response.xpath(f'//*[@id="GridView1"]//tr[{row}]/td[2]/text()'))
                    applicant_name = self.safe_extract_first(response.xpath(f'//*[@id="GridView1"]//tr[{row}]/td[3]/text()'))
                    request_period = self.safe_extract_first(response.xpath(f'//*[@id="GridView1"]//tr[{row}]/td[4]/text()'))
                    requested_days = self.safe_extract_first(response.xpath(f'//*[@id="GridView1"]//tr[{row}]/td[5]/text()'))
                    url_data['employment_requested'].append({
                        'sno': sno,
                        'demand_id': demand_id,
                        'applicant_name': applicant_name,
                        'request_period': request_period,
                        'requested_days': requested_days
                    })

            name_rows = response.xpath('//*[@id="GridView2"]//tr')
            if len(name_rows) > 0:
                for row in range(4, len(name_rows)+1, 2):
                    sno = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[1]/text()'))
                    demand_id = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[2]/text()'))
                    applicant_name = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[3]/text()'))
                    request_period = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[4]/text()'))
                    requested_days = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[5]/text()'))
                    work_name = self.safe_extract_first(response.xpath(f'//*[@id="GridView2"]//tr[{row}]/td[6]/a/text()'))
                    url_data['employment_offered'].append({
                        'sno': sno,
                        'demand_id': demand_id,
                        'applicant_name': applicant_name,
                        'request_period': request_period,
                        'requested_days': requested_days,
                        'work_name': work_name
                    })

            name_rows = response.xpath('//*[@id="GridView3"]//tr')

            if len(name_rows) > 0:
                for row in range(4, len(name_rows), 2):
                    sno = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[1]/text()'))
                    applicant_name = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[2]/text()'))
                    request_month_year = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[3]/text()'))
                    requested_days = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[4]/text()'))
                    work_name = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[5]/a/text()'))
                    msr_no = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[6]/a/text()'))
                    payment_earned = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[7]/text()'))
                    payment_due = self.safe_extract_first(response.xpath(f'//*[@id="GridView3"]//tr[{row}]/td[8]/text()'))
                    
                    url_data['employment_given'].append({
                        'sno': sno,
                        'applicant_name': applicant_name,
                        'request_month_year': request_month_year,
                        'requested_days': requested_days,
                        'work_name': work_name,
                        'msr_no': msr_no,
                        'payment_earned': payment_earned,
                        'payment_due': payment_due
                    })

            self.all_data.append(url_data)
        except Exception as e:
            logging.error(f"Error parsing {url}: {e}")
            self.not_scraped_data.append({'url': url, 'reason': str(e)})
        finally:
            self.save_to_json(self.json_filename, self.all_data)
            self.save_to_json(self.not_scraped_filename, self.not_scraped_data)
