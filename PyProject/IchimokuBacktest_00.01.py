#================================================================
#VERSION
#================================================================
#00.01			Working concept

#================================================================

import pyodbc
import time

conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='LAPTOP-D6TKOBQR\SQLEXPRESS',DATABASE='stockdb',Trusted_connection='yes')
crsr = conn.cursor()


sql = 	"""
		UPDATE Ichimoku set dtSellDate = (select top 1 SellDate
from ICHIMOKUSELL IMS
JOIN	(	SELECT top 1 *
			FROM ICHIMOKU
			WHERE bitOpenPosition is Null
			ORDER BY ID ASC
		) ICH ON ICH.strtick = IMS.strTick
WHERE IMS.selldate > (	SELECT top 1 dtBuyDate
			FROM ICHIMOKU
			WHERE bitOpenPosition is Null
			ORDER BY ID ASC
		)),
		decSellCost = (select top 1 SellPrice
from ICHIMOKUSELL IMS
JOIN	(	SELECT top 1 *
			FROM ICHIMOKU
			WHERE bitOpenPosition is Null
			ORDER BY ID ASC
		) ICH ON ICH.strtick = IMS.strTick
WHERE IMS.selldate > (	SELECT top 1 dtBuyDate
			FROM ICHIMOKU
			WHERE bitOpenPosition is Null
			ORDER BY ID ASC
		)),
		bitOpenPosition = 1
where ID = (SELECT top 1 ID
			FROM ICHIMOKU
			WHERE bitOpenPosition is Null
			ORDER BY ID ASC)
			"""

rows = crsr.execute(sql)
i = 8015
while i > 0:	
	rows = crsr.execute(sql)
	conn.commit()
	print(i)
	i = i-1

print('done')
exit()