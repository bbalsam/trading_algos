#=========================================================================================================================
#VERSIONS
#DiceRoll_Backtest_00.01.py..............................

#=========================================================================================================================
environment='TEST'
# try:
	# print('Environment is in: ' + environment)
# except:
	# environment = input('Please define the environment: ')
	# print('Environment is in: ' + environment)

if environment == 'PROD':
	emailto = 'bjaddie@gmail.com'
else:
	emailto = ''

print('test step 01')
import pyodbc
import pandas as pd
import xlwt
import datetime
import win32com.client
from win32com.client import Dispatch, constants

print('test step 02')
date = datetime.date.today()
year = str(date.year)
month = str(date.month)

while len(month) < 2:
	month = '0' + month

day = str(date.day)

while len(day) < 2:
	day = '0' + day
print('test step 03')
filelocation = str(r'C:\Users\brett\OneDrive\Documents\Excel\Trading\PyProject\Backtest\DiceRoll\\')

#filename = str(year + month + day + '_DICE_ROLL_WEEK_' + environment + '.xlsx')
filename = str(year + month + day + '_DICE_ROLL_' + environment + '.txt')

file = filelocation + filename

subject = 'Daily stock recommendations for ' + year + '/' + month + '/' + day
print('test step 04')
#writer = pd.ExcelWriter(file,engine='xlsxwriter',options={'strings_to_numbers': True})
f = open(file, 'a')
startdate = datetime.datetime(2011, 1, 15)

print(startdate.strftime('%m/%d/%Y'))
try:
	cnxn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
	cursor = cnxn.cursor()
	#Create DiceRoll
	while startdate <= datetime.datetime(2020, 12, 1):
		script = """
		USE stockdb
		declare @startdate as date
		set @startdate = '"""+startdate.strftime('%m/%d/%Y')+"""'


		IF OBJECT_ID('Q1', 'u') IS NOT NULL
		BEGIN
		DROP TABLE Q1
		END
		SELECT
			snp500.*,
			LAG(decadjClose,1) OVER (PARTITION BY snp500.strTick ORDER BY dtDate ASC) AS PrevClose
		INTO
			Q1
		FROM
			snp500
		JOIN
			(SELECT strTick FROM snp500 WHERE dtdate <= DATEADD(DAY, -365.25*5, @startdate) GROUP BY strTick) sl ON sl.strTick = snp500.strTick
		JOIN
			CompanyList CL on CL.strTick = snp500.strTick

		IF OBJECT_ID('Q2', 'u') IS NOT NULL
		BEGIN
		DROP TABLE Q2
		END
		SELECT
			*,
			decadjClose/PrevClose AS DOD_CHG
		INTO
			Q2
		FROM
			Q1
		WHERE
			Q1.PrevClose IS NOT NULL
			AND Q1.dtDate >= DATEADD(DAY, -365.25*5, @startdate)
			AND Q1.dtDate <= @startdate

		IF OBJECT_ID('Q3', 'u') IS NOT NULL
		BEGIN
		DROP TABLE Q3
		END

		SELECT TOP 20
			strTick,
			AVG(DOD_CHG) AvgDOD_CHG
		INTO
			Q3
		FROM
			Q2
		GROUP BY
			strTick
		ORDER BY
			2 DESC

		IF OBJECT_ID('Q4', 'u') IS NOT NULL
		BEGIN
		DROP TABLE Q4
		END
		SELECT
			strtick,
			MAX(Dtdate) AS MAXDATE,
			MIN(dtdate) AS MINDATE
		INTO
			Q4
		FROM
			Q1
		WHERE
			 Q1.dtDate >  @startdate
			AND Q1.dtDate <= DATEADD(DAY, 365.25, @startdate)
		GROUP BY
			strTick

		IF OBJECT_ID('Q5', 'u') IS NOT NULL
		BEGIN
		DROP TABLE Q5
		END
		SELECT
			snp.strtick,
			Q3.AvgDOD_CHG,
			SUM(CASE
				WHEN snp.dtdate = Q4.MINDATE
				THEN snp.decClose
				ELSE 0
			END) AS MINDATE_CLOSE,
			SUM(CASE
				WHEN snp.dtdate = Q4.MAXDATE
				THEN snp.decClose
				ELSE 0
			END) AS MAXDATE_CLOSE,
			SUM(CASE
				WHEN snp.dtdate = Q4.MAXDATE
				THEN snp.decClose
				ELSE 0
			END)/
			SUM(CASE
				WHEN snp.dtdate = Q4.MINDATE
				THEN snp.decClose
				ELSE 0
			END) AS GAIN_LOSS
		INTO
			Q5
		FROM
			snp500 snp
		JOIN
			Q4 ON Q4.strTick = snp.strTick
		JOIN
			Q3 ON Q3.strTick = snp.strTick
		GROUP BY
			snp.strTick,
			Q3.AvgDOD_CHG
		"""
		cursor.execute(script)
		script = """
		SELECT
			'"""+startdate.strftime('%m/%d/%Y')+"""' AS StartDate,
			AVG(GAIN_LOSS) AS YOY_GAIN_LOSS
		FROM
			Q5
		"""
		cursor.execute(script)	

		columns = [desc[0] for desc in cursor.description]
		print(columns)
		data = cursor.fetchall()
		print(data)
		df1 = pd.DataFrame.from_records(data=data)#,columns=columns)
		#df1.to_excel(writer, sheet_name=str('DiceRollBacktest'),startcol=0,index=False)
		df1.to_csv(file, header=None, index=None, sep=' ', mode='a')
		
		print('DiceRoll completed for '+ startdate.strftime('%m/%d/%Y'))	
		startdate=startdate+datetime.timedelta(days=7)
				
	
	
	#writer.save()
	print('File saved')	
	#writer.close()

	# Email	source: https://gist.github.com/ITSecMedia/b45d21224c4ea16bf4a72e2a03f741af


	# const=win32com.client.constants
	# olMailItem = 0x0
	# obj = win32com.client.Dispatch("Outlook.Application")
	# newMail = obj.CreateItem(olMailItem)
	# newMail.Subject = subject
	# newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
	# newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
	# <p>Brandon,</p>
	# <p>See attached daily recommendations report.</p>
	# <p>Thanks,</p>
	# <p><b>Brett Balsam</b><br></p>
	# </span><span style="font-size:14px; color: gray; font-family: 'Calibri Light', sans-serif;">	14809 Carriage Pl. Dr. | Burnsville, MN  55306<br>
	# c) 612-419-1544<br>
	# <a href="brettbalsam@gmail.com">brettbalsam@gmail.com</a>
	# </span><span style="font-size:12px; color: green; font-family: 'Calibri Light', sans-serif;">
	# <p><i>Please consider the environment before printing this email!</i></p>
	# </span><span style="font-size:10px; color: gray; font-family: 'Calibri Light', sans-serif;"><p>
	# <i>This e-mail may contain confidential and privileged material for the sole use of the intended recipient and is for entertainment purposes only and does not constitute financial advice. Any review, use, distribution or disclosure by others is strictly prohibited. This email and the contents attached are should not be considered an offer to buy or sell. If you are not the intended recipient (or authorized to receive for the recipient), please contact the sender by reply e-mail and delete all copies of this message.</i></p>
	# </span></body></html>"""
	# newMail.To = emailto #<-----enable for production
	# newMail.CC = ""
	# newMail.BCC = "brettbalsam@gmail.com"
	# attachment1 = file
	# newMail.Attachments.Add(Source=attachment1)
	# newMail.send
	print(subject + ' successfully sent')
except Exception as e:
	
	subject = 'FAILED - PROD - ' + subject

	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
	newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
	<p>The daily recommendations report failed to run properly.  See error below:</p><p>""" + str(e) + """</p></span></body></html>"""
	newMail.To = "brettbalsam@gmail.com"
	newMail.send
