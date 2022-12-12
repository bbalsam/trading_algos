#===========================================================
#VERSION
#00.01..........................Initial run
#00.02..........................Explore fix for emails sending over windows task scheduler
#===========================================================

import pyodbc
import pandas as pd
import xlwt
import datetime

date = datetime.date.today()
year = str(date.year)
month = str(date.month)

while len(month) < 2:
	month = '0' + month

day = str(date.day)

while len(day) < 2:
	day = '0' + day

filelocation = str(r'C:\Users\brett\OneDrive\Documents\Excel\Trading\PyProject\DailyReports\\')

filename = str('TEST' + year + month + day + '_RECOMMENDATIONS.xlsx')

file = filelocation + filename

subject = 'Daily stock recommendations for ' + year + '/' + month + '/' + day

writer = pd.ExcelWriter(file,engine='xlsxwriter',options={'strings_to_numbers': True})

try:
	cnxn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
	cursor = cnxn.cursor()
	
	# script = """
	# DROP TABLE ICHIMOKU_STAGE 
	# SELECT --TOP 1000
			# dtdate,
			# strTick,
			# decAdjClose,
			# decOpen,
			# (min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2 as CL,
			# (min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2 as BL,
			# (((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2)+((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2))/2 as SPAN_A,
			# (min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING))/2 as SPAN_B
	# INTO ICHIMOKU_STAGE
	# FROM snp500_test
	# WHERE dtdate >= dateadd(day,-2900,getdate())
	# ORDER BY strTick,dtDate ASC
	# """

	# cursor.execute(script)
	# cursor.commit()
	
	script = """
	delete from Ichimoku
	INSERT INTO Ichimoku (strTick,dtBuyDate,decBuyCost)
	SELECT strTick,BuyDate,BuyPrice
	FROM
	(select StrTick,
			CASE	WHEN	SPAN_A > SPAN_B
					THEN	1
					ELSE	0
			END AS BUY1,
			CASE	WHEN	decAdjClose > (LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC))
					AND		decAdjClose > (LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC))
					THEN	1
					ELSE	0
			END AS BUY2,
			CASE	WHEN	CL > BL
					THEN	1
					ELSE	0
			END AS BUY3,
			CASE	WHEN	decAdjClose > (LAG(SPAN_A,52) OVER (partition by strTick order by dtdate ASC))
					AND		decAdjClose > (LAG(SPAN_B,52) OVER (partition by strTick order by dtdate ASC))
					THEN	1
					ELSE	0
			END AS BUY4,
			CASE	WHEN	decAdjClose > (AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING))
					THEN	1
					ELSE	0
			END AS BUY5,
			dtdate as BuyDate,
			LEAD(decOpen,1) OVER (partition by strTick Order by dtDate asc) as BuyPrice

	from ICHIMOKU_STAGE
	) IBT
	WHERE IBT.buy1 = 1 AND IBT.buy2 = 1 AND IBT.buy3 = 1 AND IBT.buy4 = 1 AND IBT.BUY5 = 1
	"""

	cursor.execute(script)
	cursor.commit()
	
	script = """
	Drop table IchimokuBacktestBUY
	Select *
	into IchimokuBacktestBUY
	FROM
	(SELECT ID,strTick,BuyDate,decBuyCost,
			CASE WHEN BuyDate IS NOT NULL
				THEN LEAD(SellDate,1) OVER (partition by strTick Order by ID)
				ELSE NULL
			END AS SellDate,
			CASE WHEN decBuyCost IS NOT NULL
				THEN LEAD(SellCost,1) OVER (partition by strTick Order by ID)
				ELSE NULL
			END AS SellCost
	FROM
	(
	SELECT ichi.ID,
			ichi.strTick,
			CASE WHEN smpl.BUYSELLSTATUS = 'BUY'
			THEN ichi.dtBuyDate
			ELSE NULL
			END as BuyDate,
			CASE WHEN smpl.BUYSELLSTATUS = 'BUY'
			THEN ichi.decBuyCost
			ELSE NULL
			END as decBuyCost,
			CASE WHEN smpl.BUYSELLSTATUS = 'SELL'
			THEN ichi.dtBuyDate
			ELSE NULL
			END as SellDate,
			CASE WHEN smpl.BUYSELLSTATUS = 'SELL'
			THEN  ichi.decBuyCost
			ELSE NULL
			END as SellCost
	from Ichimoku ichi
	JOIN (

		SELECT	ID,
				strTick,
				dtBuyDate,
				LEAD(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc) as NextDt,
				LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc) as PrevDate,
				CASE
					WHEN	DATEDIFF(day, dtBuyDate, LEAD(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc))>5
						THEN 'SELL'
					WHEN	DATEDIFF(day, dtBuyDate, LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc))<-5
							OR	DATEDIFF(day, dtBuyDate, LAG(dtBuyDate,1) OVER (partition by strTick ORDER BY dtBuyDate asc)) IS NULL
						THEN 'BUY'
					ELSE 'HOLD'
				END AS BUYSELLSTATUS
		FROM	Ichimoku
		) smpl ON smpl.ID = ichi.ID
	WHERE smpl.BUYSELLSTATUS <> 'HOLD'
	) bs) fin
	WHERE fin.buyDate is not null
			--and fin.decBuyCost <> 0
			--AND fin.SellCost <> 0
	"""

	cursor.execute(script)
	cursor.commit()
	
	script = """
	/*VIEW TODAY'S BUY SELL RECOMMENDATIONS*/
	declare @daysback as int
	set @daysback = 0
	Select	CASE
				WHEN SellDate is NULL
				THEN 'BUY'
				ELSE 'SELL'
			END AS Recommendation,
			strTick,
			dateadd(day,1,BuyDate) as BuyDate,
			CASE
				WHEN decBuyCost is null
				THEN 'OPEN'
				ELSE CAST(decBuyCost as varchar)
			END as BuyPrice,
			/*CASE
			WHEN ROUND(1000/decBuyCost,0) = 0
			THEN 1
			ELSE ROUND(1000/decBuyCost,0)
			END as Shares,*/
			dateadd(day,1,SellDate) as SellDate,
			CASE
				WHEN SellDate is NULL
				THEN NULL
				ELSE CASE
						WHEN SellCost is null
						THEN 'OPEN'
						ELSE CAST(SellCost as varchar)
					END 
				END as SellCost
	FROM	IchimokuBacktestBUY
	where 	BuyDate >= (select dateadd(day,@daysback,max(buydate)) from IchimokuBacktestBUY)
			OR SellDate >= (select dateadd(day,@daysback,max(buydate)) from IchimokuBacktestBUY)
	order by CASE
				WHEN SellDate is NULL
				THEN 'BUY'
				ELSE 'SELL'
			END ASC,
			buydate DESC,
			strtick ASC
	"""

	cursor.execute(script)
	columns = [desc[0] for desc in cursor.description]
	data = cursor.fetchall()
	df1 = pd.DataFrame.from_records(data=data,columns=columns)
	df1.to_excel(writer, sheet_name=str('BuySellRecommendations'),startcol=0,index=False)

	script = """
	declare @daysback as int
	set @daysback = 0
	Select * from (
	Select Ticker,CompanyName,Sector,WinningRec,TotalRec,FORMAT(PctWinRec, 'P2') as PctWinRec,FORMAT((PNLPct),'P2') as PNLPct,CAST(PreviousClose as decimal(12,2)) as PreviousClose
			,100 *(((1+PNLPct)*PctWinRec)-(1-PctWinRec)/(1+PNLPct)) as KellyCriterion
	from
	(SELECT	ibuy.strTick Ticker,
			cl.strCompanyName CompanyName,
			sl.strSectorName Sector,
			SUM(CASE WHEN SellCost > decBuyCost
			THEN 1
			ELSE 0
			END) as WinningRec,
			COUNT(*) AS TotalRec,
			CASE WHEN CAST((SUM(CASE WHEN SellCost > decBuyCost THEN 1 ELSE 0 END)) AS DECIMAL)/Cast(count(*) AS DECIMAL) = 1
				THEN CAST((SUM(CASE WHEN SellCost > decBuyCost THEN 1 ELSE 0 END)) AS DECIMAL)/Cast(count(*)+1 AS DECIMAL)
				ELSE CAST((SUM(CASE WHEN SellCost > decBuyCost THEN 1 ELSE 0 END)) AS DECIMAL)/Cast(count(*) AS DECIMAL)
			END AS PctWinRec,
			AVG((SellCost/decBuyCost)-1) AS PNLPct,
			--Avg((SellCost-decBuyCost)) as PNLProfitPerShare,
			decadjclose as PreviousClose/*,
			Format(StopLoss/100,'P2') as StopLoss--NEED TO WORK OUT BUGS*/
	FROM	IchimokuBacktestBUY ibuy
	JOIN		(	SELECT	dtdate,
							ich.strTick,
							decadjclose,
							CASE WHEN CAST(ROUND(((BL/decAdjClose) - 1)*100,1) AS decimal(6,2)) > 0
							THEN CAST(ROUND((((LAG(ich.SPAN_A,26) OVER (partition by ich.strTick order by ich.dtdate ASC))/ich.decAdjClose) - 1)*100,1) AS decimal(6,2))
							ELSE CAST(ROUND(((BL/decAdjClose) - 1)*100,1) AS decimal(6,2))
							END AS StopLoss
					FROM	ICHIMOKU_STAGE ich
					JOIN	(	SELECT	strtick
								FROM	IchimokuBacktestBUY
								WHERE 	BuyDate = (	SELECT	dateadd(day,@daysback,max(buydate))
														FROM	IchimokuBacktestBUY)
									OR	SellDate = (	SELECT dateadd(day,@daysback,max(buydate))
														FROM IchimokuBacktestBUY)
													) buy
									ON buy.strtick = ich.strTick
								WHERE dtDate = (SELECT dateadd(day,@daysback,max(buydate)) FROM IchimokuBacktestBUY)
				)			stplss	ON	stplss.strTick	=	ibuy.strTick
	LEFT JOIN	CompanyList cl		ON	cl.strTick		=	ibuy.strTick
	LEFT JOIN	SectorList	sl		ON	sl.ID			=	cl.SectorListID
	Group By	ibuy.strTick,		
				StopLoss,
				decadjclose,
				cl.strCompanyName,
				sl.strSectorName
	) klly ) kc
	Order by KellyCriterion desc
	"""

	cursor.execute(script)
	columns = [desc[0] for desc in cursor.description]
	data = cursor.fetchall()
	df2 = pd.DataFrame.from_records(data=data,columns=columns)
	df2.to_excel(writer, sheet_name=str('BuyWeighting'),startcol=0,index=False)

	writer.save()
	#writer.close()

	# Email	source: https://gist.github.com/ITSecMedia/b45d21224c4ea16bf4a72e2a03f741af
	import win32com.client
	from win32com.client import Dispatch, constants

	const=win32com.client.constants
	olMailItem = 0x0
	obj = win32com.client.Dispatch("Outlook.Application")
	newMail = obj.CreateItem(olMailItem)
	newMail.Subject = subject
	newMail.BodyFormat = 2 # olFormatHTML https://msdn.microsoft.com/en-us/library/office/aa219371(v=office.11).aspx
	newMail.HTMLBody = """<html><body><span style="font-family: 'Calibri Light', sans-serif;">
	<p>Brandon,</p>
	<p>See attached daily recommendations report.</p>
	<p>Thanks,</p>
	<p><b>Brett Balsam</b><br></p>
	</span><span style="font-size:14px; color: gray; font-family: 'Calibri Light', sans-serif;">	14809 Carriage Pl. Dr. | Burnsville, MN  55306<br>
	c) 612-419-1544<br>
	<a href="brettbalsam@gmail.com">brettbalsam@gmail.com</a>
	</span><span style="font-size:12px; color: green; font-family: 'Calibri Light', sans-serif;">
	<p><i>Please consider the environment before printing this email!</i></p>
	</span><span style="font-size:10px; color: gray; font-family: 'Calibri Light', sans-serif;"><p>
	<i>This e-mail may contain confidential and privileged material for the sole use of the intended recipient and is for entertainment purposes only and does not constitute financial advice. Any review, use, distribution or disclosure by others is strictly prohibited. This email and the contents attached are should not be considered an offer to buy or sell. If you are not the intended recipient (or authorized to receive for the recipient), please contact the sender by reply e-mail and delete all copies of this message.</i></p>
	</span></body></html>"""
	#newMail.To = "brandon.addie@ttiinc.com" #<-----enable for production
	#newMail.CC = ""
	newMail.BCC = "brettbalsam@gmail.com"
	attachment1 = file
	newMail.Attachments.Add(Source=attachment1)
	newMail.send
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
