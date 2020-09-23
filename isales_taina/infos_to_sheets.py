import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from deal_collector import Deal

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('isales_taina/insight_sales.json', scope)
client = gspread.authorize(creds)

sheet = client.open('insight_sales_hubspot').sheet1

deals = Deal(limit=100)

while True:
    deals.fetchNext()
    if deals.after == '':
        break
    time.sleep(10)

deals.generateHistory(list_=True)
    
sheet.update('A{}:H{}'.format(str(2), str(1 + len(deals.deals_timeline))), deals.deals_timeline)