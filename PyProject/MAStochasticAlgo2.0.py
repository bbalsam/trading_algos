import csv
import pyodbc
import datetime

date = datetime.date.today()
year = str(date.year)
month = str(date.month)
day = str(date.day)

while len(month) < 2:
	month = '0' + month
while len(day) < 2:
	day = '0' + day

#1/11: insert file location (make sure to have 2 backslashes after the drive name (i.e. c:\\) and at the end "\\")  <-----Make sure folder exists, if not, create it.
filelocation = str('I:\\WTT\Automation\Output\PendingToPaid\\')

#2/11: change name of file
filename = str(year + month + day + '_PendingToPaidLogPROD.csv')
file = filelocation + filename

#3/11: Change server and database parameters
conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
crsr = conn.cursor()



SQL = """
use stockdb
DECLARE @Amount INT
SET @Amount = 360
DECLARE @KFAST INT
SET @KFAST = 20
DECLARE @SampleDate date
SET @SampleDate = '2020-05-27'

SELECT	val.strTick as Tick,
		val.dtDate as TriggerDate,
		CASE
		WHEN hol.strTick is null
			THEN CAST(CAST(GETDATE() as date) as varchar)
		WHEN hol.decCostBasis = 0
			THEN 'PASS'		
		ELSE 'REPEAT'
		END as DatePurchased,
		CASE
			WHEN hol.strTick is null
				THEN cast(cast(dateadd(day,14,getdate()) as date) as varchar)
			WHEN hol.decCostBasis = 0
				THEN 'PASS'		
		ELSE 'REPEAT'
		END as ProposedSaleDate,
		CASE
			WHEN hol.strTick is null
				THEN CAST(ROUND((@amount / decAdjClose),0) as int)	
		ELSE 0
		END as Shares,
		decAdjClose as CostBasis,
		LeadClose,
		LeadClose/decAdjClose as PNL,
		MaxGain,
		MaxGain/decAdjClose as MaxPNL,
		MinGain,
		MinGain/decAdjClose as MinPNL

FROM	(
		SELECT	com.strTick,com.dtDate,com.decAdjClose,MA20,PrvClose,LeadClose,MinGain,MaxGain,
				KFAST,
				AVG(com.KFAST) OVER (ORDER BY com.strtick asc, com.dtDate ASC ROWS 2 PRECEDING) AS KSLOW
		FROM
					(SELECT	strtick,
							dtDate,
							decAdjClose,
							CAST(LAG(decAdjClose,1) OVER (ORDER BY strtick asc, dtDate ASC) AS INT) AS PrvClose,
							CAST(LEAD(decAdjClose,10) OVER (ORDER BY strtick asc, dtDate ASC) AS decimal(10,6)) AS LeadClose,
							AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING) AS PrevDay,
/*MIN*/						MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS BETWEEN CURRENT ROW AND 
9 FOLLOWING) AS MinGain,
/*MAX*/						MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS BETWEEN CURRENT ROW AND 
9 FOLLOWING) AS MaxGain,
/*MA*/						AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 
49 PRECEDING) AS MA20,
							MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS L14,
							MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS H14,
							CASE
							WHEN MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) = MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)
							THEN NULL
							ELSE
							(100 * (decAdjClose-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))) / (MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))
							END as KFAST
					FROM	snp500_test
					WHERE	dtDate >= DATEADD(DAY,-365,GETDATE())
					) com
				) val
LEFT JOIN	Holdings hol ON hol.strTick = val.strTick
WHERE	dtDate = @SampleDate/*(select MAX(dtdate) from snp500_test)*/
		AND (val.KFAST < @KFAST AND (val.decAdjClose > val.MA20 AND val.PrvClose < val.MA20))
"""
rows = crsr.execute(sql)
rowct = crsr.fetchone()
try:
	print(rowct[0])
	rows = crsr.execute(sql)
	with open(file, 'w', newline='') as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow([x[0] for x in crsr.description])  # column headers
		for row in rows:
			writer.writerow(row)
			print(row)

except Exception as e:
	print (e)