from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet
from datetime import datetime

class FeedCreator:
    filename = ""
    
    def __init__(self, filename):
        self.filename = filename
    
    def get_filename(self):
        return f'{self.filename}.xlsx'
    
    def update_excel(self, latest_prices, all_prices):
        workbook = Workbook(f'{self.filename}.xlsx')
        worksheet_latest = self.create_worksheets(workbook, 'Latest')
        worksheet_all = self.create_worksheets(workbook, 'All')
        
        self.add_records(worksheet_latest, latest_prices, False)
        self.add_records(worksheet_all, all_prices)
        
        workbook.close()
    
    def create_worksheets(self, workbook: Workbook, name):
        worksheet = workbook.add_worksheet(name=name)
        
        worksheet.write('A1', 'Date')
        worksheet.write('B1', 'ISIN')
        worksheet.write('C1', 'Fund Name')
        worksheet.write('D1', 'Currency')
        worksheet.write('E1', 'Price')
            
        return worksheet
        
    def add_records(self, worksheet: Worksheet, fund_data, sort_by_date=True):
        fund_data = sorted(fund_data, key=lambda x: x['Fund Name'])
        if sort_by_date:
            fund_data = sorted(fund_data, key=lambda x: datetime.strptime(x['Date'], '%Y-%m-%d'), reverse=True)
        
        for idx, fund in enumerate(fund_data):
            worksheet.write(f'A{idx+2}', fund['Date'])
            worksheet.write(f'B{idx+2}', fund['ISIN'])
            worksheet.write(f'C{idx+2}', fund['Fund Name'])
            worksheet.write(f'D{idx+2}', fund['Currency'])
            worksheet.write(f'E{idx+2}', fund['Price'])