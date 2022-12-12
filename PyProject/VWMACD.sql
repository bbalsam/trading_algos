use stockdb

/*=========================*/
/*START BOLLINGER*/
/*=========================*/
/*
IF OBJECT_ID('tempdb..#BOLLINGER') IS NOT NULL DROP TABLE #BOLLINGER

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
INTO #BOLLINGER
FROM snp500_test
WHERE dtdate >= '01/01/2010'
*/
/*
select * from #BOLLINGER where dtdate = (select MAX(dtdate) from snp500_test)
*/
/*=========================*/
/*END BOLLINGER*/
/*=========================*/

/*=========================*/
/*START MACD*/
/*=========================*/
/*
Moving Average Convergence Divergence (MACD) is calculated by subtracting the 26-period exponential moving average (EMA) from the 12-period EMA.
*/
/*
SELECT strtick,dtdate,decOpen,decClose,BWMACD,
		AVG(BWMACD) OVER (partition by strtick order by dtdate rows 9 preceding) as SIGNAL,
		BWMACD - AVG(BWMACD) OVER (partition by strtick order by dtdate rows 9 preceding) as HIST,
		SMA200

FROM(
select	strtick,
		dtdate,
		frst.decOpen,
		frst.decClose,
		avg(BolWeightedClose) OVER (partition by strtick order by dtdate rows 12 preceding) -
		avg(BolWeightedClose) OVER (partition by strtick order by dtdate rows 25 preceding) as BWMACD,
		avg(frst.decClose) OVER (partition by strtick order by dtdate rows 199 preceding) SMA200
		
FROM(
select *,decClose*BollBand21 as BolWeightedClose
from #BOLLINGER
where BollBand21 is not null
) frst
) snd
where strtick like 'F' and dtdate >= '1/1/2000'
ORDER BY dtdate asc
*/


SELECT strtick,dtdate,decOpen,decClose,VWMACD,
		AVG(VWMACD) OVER (partition by strtick order by dtdate rows 9 preceding) as SIGNAL,
		VWMACD - AVG(VWMACD) OVER (partition by strtick order by dtdate rows 9 preceding) as HIST,
		SMA200

FROM(
select	strtick,
		dtdate,
		frst.decOpen,
		frst.decClose,
		avg(VolWeightedClose) OVER (partition by strtick order by dtdate rows 12 preceding) -
		avg(VolWeightedClose) OVER (partition by strtick order by dtdate rows 25 preceding) as VWMACD,
		avg(frst.decClose) OVER (partition by strtick order by dtdate rows 20 preceding) SMA200
		
FROM(
select *,decClose*intVol as VolWeightedClose
from snp500_test
) frst
) snd
where strtick like 'carr' and dtdate >= '1/1/2020'
ORDER BY dtdate asc
