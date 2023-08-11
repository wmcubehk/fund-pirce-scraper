from scrape_task import *
import asyncio
from aiostream import stream
import json

class Scraper:
    # A dictionary containing all possible scraping tasks as well as its isin as key
    task_dict : list[ScrapeTask] = []

    loop = asyncio.get_event_loop()

    # Register scraping tasks to be executed by the scraper
    def __init__(self) -> None:
        self.task_dict.append(CisiScrapeTask())

        self.task_dict.append(DachengScrapeTask())

        self.task_dict.append(CiccScrapeTask(class_='A'))
        self.task_dict.append(CiccScrapeTask(class_='I'))

        self.task_dict.append(AvenueScrapeTask(class_='A'))
        self.task_dict.append(AvenueScrapeTask(class_='B'))

        bochk_isin_list = ['HK0000140365', 'HK0000140340', 'HK0000441706', 'HK0000324837', 'HK0000324845', 'HK0000324894', 'HK0000388840', 'HK0000388873', 'HK0000302999', 'HK0000140357', 'HK0000206646', 'HK0000277647', 'HK0000345766', 'HK0000345824', 'HK0000210283', 'HK0000210291', 'HK0000730819', 'HK0000730827', 'HK0000730892', 'HK0000122496', 'HK0000484078', 'HK0000484177', 'HK0000484086', 'HK0000484102', 'HK0000153855', 'HK0000153848', 'HK0000441698']
        for isin in bochk_isin_list:
            self.task_dict.append(BochkScrapeTask(isin))

        with open('fund_list/allianz.json', 'r') as file:
            for fund in json.loads(file.read()):
                self.task_dict.append(AllianzScrapeTask(fund['ISIN'], fund['fund_name']))

    def is_running(self):
        return self.loop.is_running()

    # Return scraping result from selected fund houses
    # Scraper.scrape(FundHouse.A, FundHouse.B, FundHouse.C) will return fund information from A, B and C
    # Scraper.scrape() will return all tasks that is registered in this scraper
    def scrape(self):
        if self.is_running(): return

        results = self.loop.run_until_complete(self.scrape_async(self.task_dict))

        return results
    
    # Initiate the scrape tasks asynchronously
    async def scrape_async(self, task_list: list[ScrapeTask]):
        results = []
        
        coroutines = stream.merge(*[task.scrape() for task in task_list])
        
        async with coroutines.stream() as streamer:
            async for result in streamer:
                results.append(result)
        
        await ScrapeTask.close_session()

        return results
    
    def close(self):
        if self.is_running():
            print("It is still scraping!")
            return
        
        asyncio.run(self.async_close())
        self.loop.close()
        
    async def async_close(self):
        await ScrapeTask.close_session()