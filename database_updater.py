from supabase import create_client, Client
from postgrest.exceptions import APIError
from datetime import date


class Database():
    URL = "https://huswyzgpxrrzfpoqkzaw.supabase.co"
    KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1c3d5emdweHJyemZwb3FremF3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTY4NTY5OTg3NiwiZXhwIjoyMDAxMjc1ODc2fQ.Z_17fR45Q5k11hveaB9hFRnRqPwqMSbvrSJJOUYDrvU"
    supabase: Client

    # Connect to database hosted in supabase
    def __init__(self):
        self.supabase = create_client(self.URL, self.KEY)

    # Insert a scraped price record to the database
    def insert_record(self, date: date, isin: str, name: str, currency: str, price: float, *args):
        try:
            self.supabase.table('fund_prices')\
                .insert({"Date": date.strftime('%Y-%m-%d'),
                        "ISIN": isin,
                        "Fund Name": name,
                        "Currency": currency,
                        "Price": price,
                        "Daily Return": None,
                        "YTD":  None,
                        }, count=len, returning='representation').execute()
        except APIError:
            return False
        except Exception as e:
            raise e
        else:
            return True

    # Delete all records that is recorded more than 1 month and 2 weeks before
    def clean_outdated_record(self):
        self.supabase.rpc('delete_outdated_data', {}).execute()

    # Retrieve latest prices for each fund
    def get_latest_fund_prices(self):
        return self.supabase.rpc('get_latest_prices', {}).execute().data
    
    # Retrieve all prices for each fund
    def get_all_fund_prices(self):
        return self.supabase.table("fund_prices").select('Date, ISIN, "Fund Name", Currency, Price').execute().data