from scraper import Scraper
from database_updater import Database
from email_sender import EmailSender
from feed_creator import FeedCreator
import sys

send_email = False
if len(sys.argv) > 1:
    send_email = sys.argv[1] == '-email' or sys.argv[1] == '-e'


scraper = Scraper()
db = Database()
feed_creator = FeedCreator('nav_history')

results = scraper.scrape()
scraper.close()

for result in results:
    if result:
        db.insert_record(*result)

db.clean_outdated_record()

feed_creator.update_excel(latest_prices=db.get_latest_fund_prices(), all_prices=db.get_all_fund_prices())

if send_email:
    EmailSender('intern@wmcubehk.com', 'ops@wmcubehk.com').send(feed_creator.get_filename())