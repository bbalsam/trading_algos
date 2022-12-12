/*================================================
Ichimoku Cloud
================================================
Conversion Line (tenkan sen)= 
((PH 9) + (PL 9))/2
​	 
Base Line (kijun sen)= 
((PH 26) + (PL 26))/2
​	 
Leading Span A (senkou span A)= 
(CL+BL)/2

Leading Span B (senkou span B)= 
((PH 52) + (PL 52))/2
​	 
Lagging Span (chikou span)=
Close plotted 26 periods in the past

where:
PH=Period high
PL=Period low
CL=Conversion line
​================================================*/

/*================================================
Ichimoku Cloud
================================================*/
DROP TABLE ICHIMOKU_STAGE 
SELECT --TOP 1000
		dtdate,
		strTick,
		decAdjClose,
		decOpen,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2 as CL,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2 as BL,
		(((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2)+((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2))/2 as SPAN_A,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING))/2 as SPAN_B
INTO ICHIMOKU_STAGE
FROM snp500_test
WHERE dtdate >= dateadd(day,-2900,getdate())
ORDER BY strTick,dtDate ASC


/*
select	top 100000 dtdate,
		strTick,
		SPAN_A,
		SPAN_B,
		decAdjClose,
		LAG(decAdjClose,26) OVER (partition by strTick order by dtdate ASC) as LAGClose26,
		CL,
		BL,
		decOpen,
		LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC) as LAGSpanA26,
		LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC) as LAGSpanB26
		--,
		--LS_BUY,
		--LS_SELL
from ICHIMOKU_STAGE
*/
/*


/*================================================
Conversion Line (kenkan sen) (CL)
((PH9) + (PL9))/2
================================================*/

select top 1000
		dtDate,
		decHigh,
		decLow,
		min(decLow) OVER (order by dtdate ASC ROWS 8 PRECEDING) as PL9,
		max(decHigh) OVER (order by dtdate ASC ROWS 8 PRECEDING) as PH9,
		(min(decLow) OVER (order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 8 PRECEDING))/2 as CL
from snp500_test
where strTick in ('aapl')
order by dtDate desc

/*================================================
Base Line (kijun sen) (BL)
((PH 26) + (PL 26))/2
================================================*/
select top 1000
		dtDate,
		decHigh,
		decLow,
		min(decLow) OVER (order by dtdate ASC ROWS 25 PRECEDING) as PL25,
		max(decHigh) OVER (order by dtdate ASC ROWS 25 PRECEDING) as PH25,
		(min(decLow) OVER (order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 25 PRECEDING))/2 as BL
from snp500_test
where strTick in ('aapl')
order by dtDate desc
/*================================================
Leading Span A (senkou span A)
(CL+BL)/2
================================================*/
select top 1000
		dtDate,
		decHigh,
		decLow,
		(min(decLow) OVER (order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 8 PRECEDING))/2 as CL,
		(min(decLow) OVER (order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 25 PRECEDING))/2 as BL,
		(((min(decLow) OVER (order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 8 PRECEDING))/2)+((min(decLow) OVER (order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 25 PRECEDING))/2))/2 as SPAN_A
from snp500_test
where strTick in ('aapl')
order by dtDate desc
/*================================================
Leading Span B (senkou span B)
((PH 52) + (PL 52))/2
================================================*/
select top 1000
		dtDate,
		decHigh,
		decLow,
		min(decLow) OVER (order by dtdate ASC ROWS 51 PRECEDING) as PL52,
		max(decHigh) OVER (order by dtdate ASC ROWS 51 PRECEDING) as PH52,
		(min(decLow) OVER (order by dtdate ASC ROWS 51 PRECEDING) + max(decHigh) OVER (order by dtdate ASC ROWS 51 PRECEDING))/2 as SPAN_B
from snp500_test
where strTick in ('aapl')
order by dtDate desc
/*================================================
Lagging Span (chikou span)
Closing price plotted 26 days in the past 
(Modified as the (CP / LAG High 26)-1
================================================*/
select top 1000
		dtDate,
		decHigh,
		LAG(decHigh,25) OVER(partition by strTick order by dtdate) as LAG_High26,
		decAdjClose,
		(decAdjClose/LAG(decHigh,25) OVER(partition by strTick order by dtdate))-1 as LS
from snp500_test
where strTick in ('aapl')
order by dtDate desc


*/

/*================================================
Ichimoku Backtest STEP 1 insert BUY
BUY 1 logic:
Current price cloud is green
BUY 2 logic:
Current price above cloud (-26)
BUY 3 logic:
CL > BL
BUY 4 logic:
Current price > cloud (-52)
================================================*/
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


/*COMPARE TO SNP500*/
Select snp.dtDate,snp.decAdjClose,dc.DailyCt,AVG(SUM(dc.DailyCt)) OVER (ORDER BY dc.buydate ASC ROWS 20 PRECEDING) as MA20,avgPnL
FROM (select BuyDate,count(*) as DailyCt,avg(sellcost/decBuyCost) as avgPnL
	from IchimokuBacktestBUY
	where buydate > '01/01/2000'
	GROUP BY BuyDate) dc
JOIN snp500_test snp ON snp.dtDate = dc.BuyDate
where strTick = '^GSPC'
GROUP BY snp.dtDate,snp.decAdjClose,dc.buydate,dc.DailyCt,dc.avgPnL
ORDER BY dtdate

/*VIEW DAILY OPPORTUNITIES OVER DATE SPAN*/
Select	'PASS' as Status,strTick,BuyDate,decBuyCost,
		CASE
		WHEN ROUND(1000/decBuyCost,0) = 0
		THEN 1
		ELSE ROUND(1000/decBuyCost,0)
		END as Shares,SellDate,SellCost
FROM	IchimokuBacktestBUY
where buydate between dateadd(day,0,getdate()) and dateadd(day,0,getdate())
		AND strTick not in ('^GSPC')
order by buydate desc

/*VIEW TODAY'S OPPORTUNITIES*/
Select	'PASS' as Status,strTick,BuyDate,decBuyCost,
		CASE
		WHEN ROUND(1000/decBuyCost,0) = 0
		THEN 1
		ELSE ROUND(1000/decBuyCost,0)
		END as Shares,SellDate,SellCost
FROM	IchimokuBacktestBUY
where buydate > dateadd(day,-3,GETDATE())
		AND strTick not in ('^GSPC')
order by buydate desc

/*VIEW TODAY'S BUY SELL RECOMMENDATIONS*/
declare @daysback as int
set @daysback = -10
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

/* get stop loss for daily list
select dtdate,ich.strTick,decadjclose,CAST(ROUND(((BL/decAdjClose) - 1)*100,1) as decimal(6,2)) as StopLoss
from ICHIMOKU_STAGE ich
JOIN (Select	strtick
FROM	IchimokuBacktestBUY
where 	BuyDate >= (select dateadd(day,0,max(buydate)) from IchimokuBacktestBUY)) buy
		ON buy.strtick = ich.strTick
where dtDate = (select dateadd(day,0,max(buydate)) from IchimokuBacktestBUY)
*/

/*KELLY CRITERION FOR DAILY RECOMMENDATIONS*/
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


/*================================================
Ichimoku Backtest STEP 2 update SELL
Sell logic:
IF(decAdjClose<LAGClose26,1,0)
================================================*/
delete from IchimokuShort
INSERT INTO IchimokuShort (strTick,dtBuyDate,decBuyCost)
SELECT strTick,BuyDate,BuyPrice
FROM
(select StrTick,
		CASE	WHEN	SPAN_A < SPAN_B
				THEN	1
				ELSE	0
		END AS SHORT1,
		CASE	WHEN	decAdjClose < (LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC))
				AND		decAdjClose < (LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC))
				THEN	1
				ELSE	0
		END AS SHORT2,
		CASE	WHEN	CL < BL
				THEN	1
				ELSE	0
		END AS SHORT3,
		CASE	WHEN	decAdjClose < (LAG(SPAN_A,52) OVER (partition by strTick order by dtdate ASC))
				AND		decAdjClose < (LAG(SPAN_B,52) OVER (partition by strTick order by dtdate ASC))
				THEN	1
				ELSE	0
		END AS SHORT4,
		CASE	WHEN	decAdjClose < (AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING))
				THEN	1
				ELSE	0
		END AS SHORT5,
		dtdate as BuyDate,
		LEAD(decOpen,1) OVER (partition by strTick Order by dtDate asc) as BuyPrice

from ICHIMOKU_STAGE
) IBT
WHERE IBT.SHORT1 = 1 AND IBT.SHORT2 = 1 AND IBT.SHORT3 = 1 AND IBT.SHORT4 = 1 AND IBT.SHORT5 = 1


Drop table IchimokuBacktestSell
Select *
into IchimokuBacktestSell
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
from IchimokuShort ichi
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
	FROM	IchimokuShort
	) smpl ON smpl.ID = ichi.ID
WHERE smpl.BUYSELLSTATUS <> 'HOLD'
) bs) fin
WHERE fin.buyDate is not null
		--and fin.decBuyCost <> 0
		--AND fin.SellCost <> 0

select BuyDate,count(*) from IchimokuBacktestSell GROUP BY BuyDate ORDER BY BuyDate

/*COMPARE TO SNP500*/
Select snp.dtDate,snp.decAdjClose,dc.DailyCt,AVG(SUM(dc.DailyCt)) OVER (ORDER BY dc.buydate ASC ROWS 49 PRECEDING) as MA50,avgPnL
FROM (select BuyDate,count(*) as DailyCt,avg(decBuyCost/sellcost) as avgPnL
	from IchimokuBacktestSell
	where buydate > '01/01/2000'
	GROUP BY BuyDate) dc
JOIN snp500_test snp ON snp.dtDate = dc.BuyDate
where strTick = '^GSPC'
GROUP BY snp.dtDate,snp.decAdjClose,dc.buydate,dc.DailyCt,dc.avgPnL
ORDER BY dtdate desc

/*VIEW Daily Count of Recommendations*/
Select *,
		AVG(BuyRec) OVER (ORDER BY ma.buydate ASC ROWS 49 PRECEDING) LONG_MA50,
		AVG(ShortRec) OVER (ORDER BY ma.buydate ASC ROWS 49 PRECEDING) SHRT_MA50,
		AVG(MMInd) OVER (ORDER BY ma.buydate ASC ROWS 49 PRECEDING) COMB_MA50
from
(select *,comb.BuyRec-comb.ShortRec as MMInd
FROM(
Select	buy.BuyDate,count(*) as ShortRec,buy.ShortRec as BuyRec
FROM	IchimokuBacktestSell sell
FULL OUTER JOIN (Select	BuyDate,count(*) as ShortRec
FROM	IchimokuBacktestBUY
Group by BuyDate) buy ON buy.BuyDate = sell.BuyDate	
Group by buy.BuyDate,buy.ShortRec
) comb
) ma
order by ma.BuyDate desc

/*VIEW DAILY OPPORTUNITIES*/
Select	*
FROM	IchimokuBacktestSell
where buydate = (select max(buyDate) from IchimokuBacktestSell)
		and SellCost is null
order by BuyDate

