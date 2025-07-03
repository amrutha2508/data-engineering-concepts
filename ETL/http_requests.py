# Python library "requests" for working with http protocols
import requests
url = "https://www.ibm.com/"
r = requests.get(url) 
status = r.status_code
headers=  r.request.headers
body =r.request.body
header  = r.headers
header['date']
header['Content-type']
r.encoding
r.text[0:100]

#webscraping_code

import requests
import sqlite3
import pandas as pd 
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

html_page = requests.get(url).text
#print(html_page[1:300])
data = BeautifulSoup(html_page,'html.parser')
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')
#col = rows[1].find_all('td')
#print("col:",col[0].contents,col[1].contents[0],col[2])
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {'Average Rank':col[0].contents[0],"Film":col[1].contents[0],"Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict,index = [0])
            df = pd.concat([df,df1],ignore_index=True)
            count = count+1
    else:
        break
df.to_csv(csv_path)


conn = sqlite3.connect(db_name)
df.to_sql(table_name,conn,if_exists = "replace",index=False)
conn.close()
