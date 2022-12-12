use StockDB
/*LIST OF VALID TRADES*/
/*
SELECT Filing_Date,trade_date,NextDate as OpenDate,PriorDate,Ticker,[Value]
FROM
	(
	SELECT	*
	FROM	InsiderPurchases	ipur
	JOIN	
			(
			SELECT	dtdate,LEAD(dtdate,1) OVER (ORDER BY dtdate ASC) NextDate,LAG(dtdate,1) OVER (ORDER BY dtdate ASC) PriorDate
			FROM	TradingDates
			)	td
			ON td.dtdate = cast(ipur.Filing_Date as date)
				AND td.PriorDate >= ipur.Trade_Date
	) fst
order by Filing_Date desc
*/
/*VALID TRADES WITH OPEN AND CLOSE PRICE*/

SELECT OpenDate,PriorDate,Sum([Value]) as SumPurAmt,SUM((ClosePrice/OpenPrice)*[Value])/Sum([Value]) as WAPNL
FROM
	(
	SELECT Filing_Date,trade_date,NextDate as OpenDate,PriorDate,Ticker,[Value]
	FROM
		(
		SELECT	*
		FROM	InsiderPurchases	ipur
		JOIN	
				(
				SELECT	dtdate,LEAD(dtdate,1) OVER (ORDER BY dtdate ASC) NextDate,LAG(dtdate,1) OVER (ORDER BY dtdate ASC) PriorDate
				FROM	TradingDates
				)	td
				ON td.dtdate = cast(ipur.Filing_Date as date)
					AND td.PriorDate <= ipur.Trade_Date
		) fst
	) snd
JOIN
		(
		SELECT dtdate,strtick,decOpen as OpenPrice,lead(decOpen,1) OVER (partition by strtick order by dtdate asc) as ClosePrice,AVG(intVol) OVER (partition by strtick order by dtdate asc ROWS 20 PRECEDING) as AvgVol
		FROM InsiderStocks
		WHERE dtDate > '2017-01-01'
		) ins
		ON ins.dtDate = snd.OpenDate AND ins.strTick = snd.Ticker
WHERE ClosePrice is not null AND AvgVol >= 50000 AND OpenPrice >=1
GROUP BY OpenDate,PriorDate
ORDER BY OpenDate DESC