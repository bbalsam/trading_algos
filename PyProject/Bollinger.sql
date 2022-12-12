
SELECT	ID,
		strTick,
		dtDate,
		CAST(decOpen as decimal(10,2)) AS decOpen,
		CAST(decHigh as decimal(10,2)) AS decHigh,
		CAST(decLow as decimal(10,2)) AS decLow,
		CAST(decAdjClose as decimal(10,2)) AS decClose,
		CAST(decAdjClose as decimal(10,2)) AS decAdjClose,
		CAST(LEAD(decOpen,1) OVER (PARTITION BY strTick ORDER BY dtDate ASC) as decimal(10,2)) as NextOpen,
		CAST(AVG(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC ROWS 20 PRECEDING) as decimal(10,2)) as SMA21,
		CAST(STDEV(decAdjClose) OVER (PARTITION BY strTick ORDER BY dtDate ASC ROWS 20 PRECEDING)*2 as decimal(10,2)) as BollBand21
FROM snp500_test
WHERE dtdate > '01/01/2020' and dtdate < '03/01/2020'