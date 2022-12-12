# importing the libraries
from bs4 import BeautifulSoup
import requests

import pyodbc
from datetime import datetime, timedelta
from pandas_datareader import data as pdr
from datetime import date

url='http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=0&fdr=&td=0&tdr=&fdlyl=&fdlyh=3&daysago=&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=5&page=1' 

# Make a GET request to fetch the raw HTML content
html_content = requests.get(url).text

# Parse the html content
soup = BeautifulSoup(html_content, 'lxml')
#print(soup.prettify()) # print the parsed data of html

#print(soup.td())

#===============================================
#ODBC connection code: https://reasonabledeviations.com/2018/02/01/stock-price-database/#database-schema
#===============================================
conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
crsr = conn.cursor()


insider_table = soup.find('table', attrs={'class': 'tinytable'})
#===============================================
#Code pulls headers, not necessary once table created
#===============================================
# insider_table_data = insider_table.thead.find_all('tr')

# Get all the headings of Lists
# headings = []
# for tr in insider_table_data[0].find_all('th'):
	# remove any newlines and extra spaces from left and right
	# print(str(tr.text))
	# print(tr.text.replace('\xa0','_'))
	# headings.append(tr.text.replace('\xa0','_')) #text..replace('\n', ' ').strip.b
	# print(tr)

# print(headings)
#===============================================
#Code pulls Insider trading data
#===============================================
insider_table_data = insider_table.tbody.find_all('tr')

# Get all the headings of Lists

tdata = []
i=0
while i <= 4: #one less than cnt in webpage html 'url'
	tdata = []
	for tr in insider_table_data[i].find_all('td'):
		# remove any newlines and extra spaces from left and right
		#print(str(tr.text))
		tdata.append(tr.text.replace('\xa0','_').replace('$','').replace('%','').replace(',','').replace('+','')) #text..replace('\n', ' ').strip.b
	tdata = tdata[:12]
	createid = tdata[1]
	tdata[3] = tdata[3].replace(' ','')
	id = (createid[:4]+createid[5:7]+createid[8:10]+'_'+createid[11:13]+createid[14:16]+createid[17:19]+'_'+str(tdata[3]))
	tdata.insert(0,id)
	print(tdata)
	i=i+1

#print(tdata)
