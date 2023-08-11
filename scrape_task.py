from bs4 import BeautifulSoup
from lxml import etree
from abc import ABC, abstractmethod
from typing import AsyncGenerator
import re
import datetime
import asyncio
import aiohttp
import random

# Parent class for all other fund-house-specifc tasks
class ScrapeTask(ABC):
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    _session = None

    # Retrieve the current AioHttp client session, create one if it doesn't exist
    @classmethod
    async def get_session(cls):
        if cls._session is None:
            cls._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        return cls._session

    # Close the current AioHttp client session
    @classmethod
    async def close_session(cls):
        if cls._session == None: return
        await cls._session.close()
        cls._session = None

    # Async of BeautifulSoup() so that it will not lose any information
    @classmethod
    async def async_beautiful_soup(self, content):
        loop = asyncio.get_event_loop()
        soup = await loop.run_in_executor(None, BeautifulSoup, content, 'lxml')
        return soup

    # Convert months from English (Jan, January, feb, february...) to integers (1, 2, 3...)
    def convert_english_month(self, x):
        months = {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr':4,
            'may':5,
            'jun':6,
            'jul':7,
            'aug':8,
            'sep':9,
            'oct':10,
            'nov':11,
            'dec':12
            }
        try:
            a = x.strip()[:3].lower()
            ez = months[a]
            return ez
        except:
            return x

    # Convert scraped data to uniform data that is ready to be uploaded to the database
    # record_date: list[year, month, day]
    def clean_fund_data(self, record_date, isin, fund_name, currency, price, *others) -> tuple[datetime.date, str, str, str, float, tuple]:
        record_date[2] = re.sub(r'[^0-9]', '', record_date[2])
        record_date[1] = self.convert_english_month(record_date[1])
        if type(price) == str:
            price = float(re.sub(r'[^0-9.]', '', price))
        return (datetime.date(int(record_date[0]), int(record_date[1]), int(record_date[2])), isin, fund_name, currency, price, *others)

    # Wait a short random amount of time before scraping
    # To be overridden by subclasses
    @abstractmethod
    async def scrape(self) -> AsyncGenerator[tuple[datetime.date, str, str, str, float, tuple], None]:
        await asyncio.sleep(random.uniform(0.5, 1.5))

#region Fund-house-specific tasks
class CisiScrapeTask(ScrapeTask):
    async def scrape(self):
        await super().scrape()

        headers = {
            'x-requested-with': 'XMLHttpRequest',
        }

        data = {
            'fid': '34'
        }

        try:
            session = await ScrapeTask.get_session()
            async with session.post('https://funds.xyzq.com.hk/fund/index/price', headers=headers, data=data) as response:
                response_json = await response.json()
                date = next(iter(response_json['msg']['fordate']))
                price = response_json['msg']['fordate'][next(iter(response_json['msg']['fordate']))]['33'][0]

                yield self.clean_fund_data(date.split('-'), 'HK0000514486', 'CISI Selection Fund Series - China Core Asset Fund Class A HKD', 'HKD', price)
        except Exception as e:
            print()
            print('Cisi:', e)
        
class DachengScrapeTask(ScrapeTask):
    async def scrape(self):
        await super().scrape()
        try:
            session = await ScrapeTask.get_session()
            async with session.get("https://www.dcfund.com.hk/en/MutualRecognitionFund/", headers=self.HEADERS) as response:
                response_text = await response.text()
                soup = await self.async_beautiful_soup(response_text)
                soup = soup.find('div', class_='products-onebtmtxt').h6.text.replace(" ", "").splitlines()
                date = soup[3].split("Date:",1)[1].split('.')
                price = soup[1][4:]
                yield self.clean_fund_data(date, 'CNE1000024K9', 'Da Cheng Domestic Demand Growth Mixed Fund H CNY', 'CNY', price)
        except Exception as e:
            print()
            print('Da Cheng:', e)
        
class CiccScrapeTask(ScrapeTask):
    soup = None
    class_ = 'A'

    def __init__(self, class_:str):
        self.class_ = class_.upper()

    @classmethod
    async def get_soup(cls):
        if cls.soup == None:
            session = await ScrapeTask.get_session()
            async with session.get("https://cicchkam.com/en/product/overview/4?pro=1", headers=cls.HEADERS) as response:
                response_text = await response.text()
                cls.soup = await cls.async_beautiful_soup(response_text)
        return cls.soup

    async def scrape(self):
        await super().scrape()

        try:
            soup = await self.get_soup()
            date = soup.find(class_='element-box-right').find(class_='element-box-text').p.span.text.split('-')
            if self.class_ == 'A':
                price = soup.find(class_='currency-panel').find_all('div', recursive=False)[2].find(class_='number').text
                yield self.clean_fund_data(date, 'HK0000651908', 'ICBC CICC USD Money Market ETF Unlisted Class A', 'USD', price)
            else:
                price = soup.find(class_='currency-panel').find_all('div', recursive=False)[4].find(class_='number').text
                yield self.clean_fund_data(date, 'HK0000651890', 'ICBC CICC USD Money Market ETF Unlisted Class I', 'USD', price)
        except Exception as e:
            print()
            print('Cicc:', e)
        
class AvenueScrapeTask(ScrapeTask):
    soup = None
    class_ = 'A'

    def __init__(self, class_:str):
        self.class_ = class_.upper()

    @classmethod
    async def get_soup(cls):
        if cls.soup == None:
            session = await ScrapeTask.get_session()
            async with session.get("https://www.avenue.limited/public-fund/", headers=cls.HEADERS) as response:
                response_text = await response.text()
                cls.soup = await cls.async_beautiful_soup(response_text)
        return cls.soup

    async def scrape(self):
        await super().scrape()

        try:
            soup = await self.get_soup()
            date = soup.find('div', {'data-id':'8218b1a'}).div.div.text.split('of ')[1].split()
            date.reverse()
            prices = soup.find(class_='elementor-heading-title elementor-size-small').get_text(separator="\n").splitlines()
            if self.class_ == 'A':
                price = prices[3].split(' ')[1]
                yield self.clean_fund_data(date, 'HK0000675881', 'Avenue Fixed Income Fund Class A HKD', 'HKD', price)
            else:
                price = prices[5].split(' ')[1]
                yield self.clean_fund_data(date, 'HK0000675899', 'Avenue Fixed Income Fund Class B USD', 'USD', price)
        except Exception as e:
            print()
            print('Avenue:', e)
        
class BochkScrapeTask(ScrapeTask):
    soup = None
    isin = ""

    def __init__(self, isin):
        self.isin = isin

    @classmethod
    async def get_soup(cls):
        if cls.soup == None:
            session = await ScrapeTask.get_session()
            async with session.get("https://www.bochkam.com/amc/listFund.action?fundType=A&lang=en#fundPrices", headers=cls.HEADERS) as response:
                response_text = await response.text()
                cls.soup = await cls.async_beautiful_soup(response_text)
        return cls.soup

    async def scrape(self):
        await super().scrape()

        try:
            soup = await self.get_soup()
            dom = etree.HTML(str(soup))
            fund_name = str(dom.xpath(f'//*[contains(text(), "{self.isin}")]/../td[1]/a/text()')[0]).strip()
            currency = str(dom.xpath(f'//*[contains(text(), "{self.isin}")]/../td[4]/text()')[0]).strip()
            date = str(dom.xpath(f'//*[contains(text(), "{self.isin}")]/../td[3]/text()')[0]).strip().split('/')
            price = str(dom.xpath(f'//*[contains(text(), "{self.isin}")]/../td[5]/text()')[1]).strip()
            
            yield self.clean_fund_data(date, self.isin, fund_name, currency, price)
        except Exception as e:
            print()
            print('Bochk:', e)
        
class AllianzScrapeTask(ScrapeTask):
    isin = ''
    fund_name = ''

    def __init__(self, isin, fund_name):
        self.isin = isin
        self.fund_name = fund_name

    async def scrape(self):
        await super().scrape()

        try:
            # Retrieve seo fund name
            data = {
                'SelectedSearchFundName': self.isin,
                'Region': 'ap',
                'SalesChannel': 'hk_retail_financial_advisor',
                'Language': 'en',
                'DatasourceId': '{177EA786-B0DF-491A-B6B5-76405CCAE5D2}',
                'selectedFundType[FundTypeKey]': 'Mutual Funds',
            }

            session = await ScrapeTask.get_session()
            async with session.post('https://hk.allianzgi.com/api/sitecore/searchservice/fundlist', data=data, headers=self.HEADERS) as response:
                response_content = await response.json()
                fund_id = response_content['FundAuto'][0]['FundId']

            # Retrieve fund price
            async with session.get(f'https://hk.allianzgi.com/en/financial-advisor/products-solutions/retail-funds/{fund_id}', headers=self.HEADERS) as response:
                response_content = await response.content.read()
                soup = await self.async_beautiful_soup(response_content)

                date = soup.find(class_='fund-summary-banner').find('span', class_='c-copy').h4.get_text().strip().split('/')
                date.reverse()
                currency, price = filter(None, soup.find(class_='fund-summary-banner').find_all('div', 'c-tile-content')[1].find('h4').get_text().strip().split(' '))

                yield self.clean_fund_data(date, self.isin, self.fund_name, currency, price)
        except Exception as e:
            print()
            print('Allianz:', e)
#endregion