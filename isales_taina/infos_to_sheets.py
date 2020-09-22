import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from deal_collector import Deal

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('isales_taina/insight_sales.json', scope)
client = gspread.authorize(creds)

sheet = client.open('insight_sales_hubspot').sheet1
last_row = len(sheet.col_values(1)) + 1

deals = Deal(limit=100)

while True:
    deals.fetchNext()
    if deals.after == '':
        break
    time.sleep(10)

deals.generateHistory()
    
sheet.update('A{}:I{}'.format(str(last_row), str(last_row + deals.n_deals - 1)), deals)