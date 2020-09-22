from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, Text
from config import host, port, database, user, password
import pandas as pd
from deal_collector import Deal
import time

conn_str = f"postgresql://{user}:{password}@{host}/{database}"
engine = create_engine(conn_str)

deals = Deal(limit=100)

while True:
    deals.fetchNext()
    if deals.after == '':
        break
    time.sleep(10)

deals.generateHistory()

deals_df = pd.DateFrame(deals.deals_timeline)
deals_df.to_sql(
   'deals', 
   con=engine,
   if_exists='replace',
   index=False,
   dtype={
      'id':Integer, 
      'createdate':DateTime, 
      'closedate':DateTime, 
      'dealstage':Text, 
      'hs_analytics_source':Text, 
      'amount':Text, 
      'num_notes':Integer, 
      'timestamp':DateTime
   }
)