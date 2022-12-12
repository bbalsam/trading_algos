
use stockdb
/*
Create InsiderPurchases table:
*/
/*
CREATE TABLE InsiderPurchases (
ID  varchar(64) unique,
X varchar(8),
Filing_Date datetime,
Trade_Date date,
Ticker varchar(8),
Company_Name varchar(64),
Insider_Name varchar(64),
Title varchar(64),
Trade_Type__ varchar(64),
Price float,
Qty float,
Owned float,
DeltaOwn float,
Value float,
);
*/
/*
select * from InsiderPurchases
*/
/*
insert into InsiderPurchases VALUES(
									'20210217_164436_CRVS'
									,''
									,CAST('2021-02-17 16:44:36' as DATETIME)
									,CAST('2021-02-17' AS DATE)
									,REPLACE(' CRVS',' ','')
									,'Corvus Pharmaceuticals Inc.'
									,'Miller Richard A Md','Pres CEO'
									,'P - Purchase'
									,CAST(REPLACE('$3.50','$','') as DECIMAL(12,2))
									,CAST(REPLACE(REPLACE('+100,000','+',''),',','') as DECIMAL(12,2))
									,CAST(REPLACE('1,278,515',',','') as DECIMAL(12,2))
									,CAST(REPLACE(REPLACE('+8%','+',''),'%','') as DECIMAL(12,2))
									,CAST(REPLACE(REPLACE(REPLACE('+$350,000','+',''),'$',''),',','') as DECIMAL(12,2)))
									*/
/*
select ipr.*,ist.OpenPrice,ist.ClosePrice,ist.ClosePrice/ist.OpenPrice as PNL
from InsiderPurchases ipr
JOIN (SELECT	*,
		LEAD(decOpen,1) OVER (partition by strtick order by dtdate) as OpenPrice,
		LEAD(decOpen,2) OVER (partition by strtick order by dtdate) as ClosePrice
FROM	InsiderStocks
WHERE	dtDate >= '2017/01/01') ist
							ON	ist.strTick = ipr.Ticker
								AND cast(ipr.filing_date as date) = ist.dtDate
where	cast(ipr.filing_date as date) <= DATEADD(DAY,1,ipr.Trade_Date)
		AND	ist.ClosePrice is not null
order by Filing_Date desc

*/

/*============================
KELLY CRITERION
==============================
print('enter the payout %:')
b = float(input())
print('enter the probibility of success:')
p = float(input())
q = 1 - p
bk = 100
print('bankroll % amount:')
print(bk*(((b*p)-q)/b))
wait = input('press any key to continue...')
*/

/*============================
Initial attempt; previous data not precise
==============================*/
/*
Select	Ticker,
		AVG(pnl) as AvgPNL,
		COUNT(*) as Times,
		(AVG(SuccessPct)) as SuccessPct,
		(AVG(WinAvgPct)) as WinAvgPct,
		(AVG(LossAvgPct)) as LossAvgPct,
		((AVG(WinAvgPct)-1)/(1-AVG(LossAvgPct))) as RiskRewardRatio,
		(100*(((((AVG(WinAvgPct)-1)/(1-AVG(LossAvgPct)))*(AVG(SuccessPct)))-(1-(AVG(SuccessPct))))/((AVG(WinAvgPct)-1)/(1-AVG(LossAvgPct))))) as KellyCriterion,
		AVG(Avg20Vol) as AVG20VOL
FROM
		(select ipr.*,
				ist.OpenPrice,ist.ClosePrice,ist.ClosePrice/ist.OpenPrice as PNL,
				CASE
					WHEN ist.ClosePrice/ist.OpenPrice >=1
					THEN 1.00
					ELSE 0.00
				END as SuccessPct,
				CASE
					WHEN ist.ClosePrice/ist.OpenPrice >=1
					THEN ist.ClosePrice/ist.OpenPrice
					ELSE NULL
				END as WinAvgPct,
				CASE
					WHEN ist.ClosePrice/ist.OpenPrice <1
					THEN ist.ClosePrice/ist.OpenPrice
					ELSE NULL
				END as LossAvgPct,
				Avg20Vol
		FROM InsiderPurchases ipr
		JOIN (SELECT	*,
				LEAD(decOpen,1) OVER (partition by strtick order by dtdate) as OpenPrice,
				LEAD(decOpen,2) OVER (partition by strtick order by dtdate) as ClosePrice,
				AVG(intvol) OVER (partition by strtick order by dtdate ROWS 20 PRECEDING) as Avg20Vol
				FROM	InsiderStocks
				WHERE	dtDate >= '2017/01/01'
			) ist
						ON	ist.strTick = ipr.Ticker
							AND cast(ipr.filing_date as date) = ist.dtDate
		WHERE	/*cast(ipr.filing_date as date) <= DATEADD(DAY,1,ipr.Trade_Date)
				AND*/	ist.ClosePrice is not null
				AND ist.strtick in (SELECT ticker
									FROM InsiderPurchases
									WHERE cast(filing_date as date) <= DATEADD(DAY,1,Trade_Date) AND Filing_Date >= DATEADD(DAY,-2,DATEADD(HOUR,15,cast(cast(getdate() as date) as datetime)))
									--AND TICKER IN ('HEPA','ILAL','NUS','ONCR','SCPS')
									GROUP BY Ticker,Filing_Date,Trade_Date)) fst

--WHERE Avg20Vol >= 50000
GROUP BY
		Ticker
--HAVING (AVG(LossAvgPct)) is not NULL
ORDER BY
		Times desc

*/
/*============================
Risk Reward analysis by Ticker
==============================*/
/*
SELECT	Ticker,AVG(PNL) as AvgPNL,
		SUM(
		CASE
			WHEN PNL >= 1.00
			THEN 1.00
			ELSE 0.00
		END)/COUNT(*)
		AS SuccessRate,
		COUNT(*) as NumOfPurchases,
		AVG(
		CASE
			WHEN PNL >= 1.00
			THEN PNL
			ELSE NULL
		END)
		AS WinAvgPct,
		AVG(
		CASE
			WHEN PNL < 1.00
			THEN PNL
			ELSE NULL
		END)
		AS LossAvgPct,
		((AVG(
		CASE
			WHEN PNL >= 1.00
			THEN PNL
			ELSE NULL
		END)-1)/(1-AVG(
		CASE
			WHEN PNL < 1.00
			THEN PNL
			ELSE NULL
		END))) as RiskRewardRatio,
		AVG(AvgVol) as AVGVol
FROM
	(	
	SELECT Ticker,ist.OpenBuy,ist.OpenSell,ist.OpenSell/ist.OpenBuy as PNL,AvgVol
	FROM InsiderPurchases ipr
	JOIN (SELECT dtdate,lag(dtdate,1) OVER(order by dtdate asc) as PrevDate FROM tradingdates) td ON td.dtdate = cast(Filing_Date as date) and td.PrevDate = ipr.Trade_Date
	JOIN (SELECT *,lead(decOpen,1) OVER (partition by strtick order by dtdate asc) as OpenBuy,lead(decOpen,2) OVER(partition by strtick order by dtdate asc) as OpenSell,AVG(intvol) OVER (partition by strtick order by dtdate asc ROWS 20 PRECEDING) as AvgVol FROM InsiderStocks) ist ON ist.dtDate = cast(Filing_Date as date) AND ist.strTick = ipr.Ticker
	--WHERE ticker in ('BWB')
	) fst
GROUP BY Ticker
*/
/*
select * from InsiderPurchases ipr
JOIN (SELECT dtdate,lag(dtdate,1) OVER(order by dtdate asc) as PrevDate FROM tradingdates) td ON td.dtdate = cast(Filing_Date as date) and td.PrevDate = ipr.Trade_Date
JOIN (SELECT *,lead(decOpen,1) OVER (partition by strtick order by dtdate asc) as OpenBuy,lead(decOpen,2) OVER(partition by strtick order by dtdate asc) as OpenSell FROM InsiderStocks) ist ON ist.dtDate = cast(Filing_Date as date) AND ist.strTick = ipr.Ticker
where ticker in ('BWB')
*/
/**/
/*============================
Misc queries
==============================*/
/*
Select *
from InsiderStocks
where strtick in ('HEPA','ILAL','NUS','ONCR')
	and dtDate = '2021-02-17'


	select * from InsiderPurchases where ticker in ('ONCR')


	*/
/*
SELECT ticker,Filing_Date,Trade_Date
FROM InsiderPurchases
WHERE cast(filing_date as date) <= DATEADD(DAY,1,Trade_Date) AND Filing_Date >= DATEADD(DAY,-1,DATEADD(HOUR,15,cast(cast(getdate() as date) as datetime)))
GROUP BY Ticker,Filing_Date,Trade_Date
ORDER BY Filing_Date

select DATEADD(DAY,-1,DATEADD(HOUR,15,cast(cast(getdate() as date) as datetime)))

*/

/*==================================================================
New attempt; with previous date solved
==================================================================*/
/*
SELECT snd.dtdate,avg(snd.AVGPNL) as AVGPNL
FROM (
		SELECT fst.dtdate,fst.strtick,avg(PNL) as AVGPNL
		FROM	(
				SELECT tds.dtdate,tds.prevdate,ipr.Ticker as strtick,stnx.intVol,PNL
				FROM InsiderPurchases ipr
				JOIN
					(SELECT dtdate,lag(dtdate,1) OVER (ORDER BY dtdate asc) PrevDate
					FROM tradingdates) tds
				ON	CAST(ipr.Filing_Date as date) = tds.dtdate
													AND ipr.Trade_Date >= tds.PrevDate
				JOIN
					(SELECT *,
							LEAD(decOpen,1) OVER (PARTITION BY strtick ORDER BY dtdate asc) as OpenPrice,
							LEAD(decOpen,2) OVER (PARTITION BY strtick ORDER BY dtdate asc) as ClosePrice,
							CASE
								WHEN LEAD(decOpen,1) OVER (PARTITION BY strtick ORDER BY dtdate asc) is null or LEAD(decOpen,1) OVER (PARTITION BY strtick ORDER BY dtdate asc) = 0
								THEN NULL
								ELSE LEAD(decOpen,2) OVER (PARTITION BY strtick ORDER BY dtdate asc)/LEAD(decOpen,1) OVER (PARTITION BY strtick ORDER BY dtdate asc)
							END as PNL
					FROM InsiderStocks
					WHERE dtdate >= '2017-01-01'
							AND intVol >=50000/**/) stnx
				ON CAST(ipr.Filing_Date as date) = stnx.dtdate
				) fst
		GROUP BY fst.dtdate,fst.strtick
	) snd
GROUP BY dtdate
*/
/*============================
Weighted trades by day
==============================*/
/*
SELECT FileDate,SUM(PNL*PurAmt)/SUM(PurAmt) as WAPNL,avg(PNL) AvgPNL,Count(*) Cnt,SUM(OpenBuy*PurAmt)/SUM(PurAmt) as WAOpenPrice,SUM(AvgVol*PurAmt)/SUM(PurAmt) as WAVol
FROM
	(	
	SELECT cast(ipr.Filing_Date as date) as FileDate,Ticker,ist.OpenBuy,ist.OpenSell,ist.OpenSell/ist.OpenBuy as PNL,AvgVol,ipr.[Value] as PurAmt
	FROM InsiderPurchases ipr
	JOIN (SELECT dtdate,lag(dtdate,1) OVER(order by dtdate asc) as PrevDate FROM tradingdates) td ON td.dtdate = cast(Filing_Date as date) and td.PrevDate = ipr.Trade_Date
	JOIN (SELECT *,lead(decOpen,1) OVER (partition by strtick order by dtdate asc) as OpenBuy,lead(decOpen,2) OVER(partition by strtick order by dtdate asc) as OpenSell,AVG(intvol) OVER (partition by strtick order by dtdate asc ROWS 20 PRECEDING) as AvgVol FROM InsiderStocks) ist ON ist.dtDate = cast(Filing_Date as date) AND ist.strTick = ipr.Ticker
	--WHERE AvgVol >=50000
	) fst
GROUP BY FileDate
ORDER BY FileDate ASC

SELECT CAST(Filing_Date as DATE) as FileDate,Ticker,SUM([Value]) as SumOfPurch--,Avg(AvgVol) as AvgVol,AVG(OpenBuy) as OpenBuy,AVG(OpenSell) as OpenSell
FROM InsiderPurchases ipr
JOIN (SELECT dtdate,lag(dtdate,1) OVER(order by dtdate asc) as PrevDate FROM tradingdates) td ON td.dtdate = cast(Filing_Date as date) and td.PrevDate = ipr.Trade_Date
--JOIN (SELECT *,lead(decOpen,1) OVER (partition by strtick order by dtdate asc) as OpenBuy,lead(decOpen,2) OVER(partition by strtick order by dtdate asc) as OpenSell,AVG(intvol) OVER (partition by strtick order by dtdate asc ROWS 20 PRECEDING) as AvgVol FROM InsiderStocks) ist ON ist.dtDate = cast(Filing_Date as date) AND ist.strTick = ipr.Ticker
GROUP BY CAST(Filing_Date as DATE),Ticker
ORDER BY CAST(Filing_Date as DATE) DESC

*/
/*============================
Trade list
==============================*/
use stockdb
declare	@forcedate		as	datetime
set		@forcedate		=	/*CASE  --Morning
								WHEN CAST(getdate() AS DATE)=(SELECT CAST(MAX(Filing_Date) AS DATE) FROM InsiderPurchases)
								THEN (SELECT LAG(CAST(Filing_Date AS DATE),1) OVER (ORDER BY Filing_Date ASC) FROM InsiderPurchases WHERE Filing_Date = (SELECT MAX(Filing_Date) FROM InsiderPurchases))
								ELSE (SELECT CAST(MAX(Filing_Date) AS DATE) FROM InsiderPurchases)
							END*/--
							(SELECT CAST(max(Filing_Date) AS DATE) FROM InsiderPurchases) --Night before
							--getdate()--
							--(SELECT @forcedate,dtdate,PrevDate FROM (SELECT *,LAG(dtdate,1) OVER (ORDER BY dtdate ASC) as PrevDate FROM TradingDates) td where CAST(@forcedate as date) = td.dtdate)

declare	@tradedatestart	as	datetime
declare	@tradedateend	as	datetime

set		@tradedatestart	=	@forcedate
							--(SELECT dateadd(hh,0,CAST(PrevDate as datetime)) FROM (SELECT *,LAG(dtdate,1) OVER (ORDER BY dtdate ASC) as PrevDate FROM TradingDates) td where CAST(@forcedate as date) = td.dtdate)
set		@tradedateend	=	DATEADD(DD,1,@forcedate)
							--(SELECT dateadd(hh,0,CAST(dtdate as datetime)) FROM (SELECT *,LAG(dtdate,1) OVER (ORDER BY dtdate ASC) as PrevDate FROM TradingDates) td where CAST(@forcedate as date) = td.dtdate)

/*select @forcedate,@tradedatestart,@tradedateend*/

/*select Ticker--,sum([Value]) as SumVal,Max(Filing_Date) as MaxFDate
from InsiderPurchases
where Filing_Date	between @tradedatestart and @tradedateend
					--between DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) and DATEADD(HOUR,7,DATEADD(DAY,1,CAST(CAST(GETDATE() as DATE) as DATETIME)))  /*select DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)),DATEADD(HOUR,7,DATEADD(DAY,1,CAST(CAST(GETDATE() as DATE) as DATETIME)))*/
					--between DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))) and DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) /*select DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))),DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME))*/
					/*MONDAY*/--between DATEADD(HOUR,7,DATEADD(DAY,-3,CAST(CAST(GETDATE() as DATE) as DATETIME))) and DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) /*select DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))),DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME))*/
GROUP BY Ticker
ORDER BY Ticker*/



SELECT				dtdate,strtick,ipr.SumVal,'' as InvstWeight,'=IF(D3=0,0,1/(COUNT($D:$D)-COUNTIF($D:$D,0)))' as EqualWeight,decopen,decclose,intVol
FROM				InsiderStocks ist
JOIN	(select Ticker,sum([Value]) as SumVal from InsiderPurchases
					where Filing_Date	between @tradedatestart and @tradedateend
										--between DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) and DATEADD(HOUR,7,DATEADD(DAY,1,CAST(CAST(GETDATE() as DATE) as DATETIME)))  /*select DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)),DATEADD(HOUR,7,DATEADD(DAY,1,CAST(CAST(GETDATE() as DATE) as DATETIME)))*/
										--between DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))) and DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) /*select DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))),DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME))*/
										/*MONDAY*/--between DATEADD(HOUR,7,DATEADD(DAY,-3,CAST(CAST(GETDATE() as DATE) as DATETIME))) and DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME)) /*select DATEADD(HOUR,7,DATEADD(DAY,-1,CAST(CAST(GETDATE() as DATE) as DATETIME))),DATEADD(HOUR,7,CAST(CAST(GETDATE() as DATE) as DATETIME))*/
					GROUP BY Ticker
					) ipr ON ipr.ticker = ist.strtick
WHERE				dtDate = (select max(dtdate) from InsiderStocks)
					and intvol >=50000
ORDER BY			dtdate desc, strtick asc

/*
SELECT max(dtdate)
FROM 
(select datepart(hh,Filing_Date) dtdate from InsiderPurchases
GROUP BY datepart(hh,Filing_Date)) fst
*/

/*
select Ticker,sum([Value]) as SumVal from InsiderPurchases
where Filing_Date between '2021-03-09 00:00:00' and '2021-03-10 00:00:00'
GROUP BY Ticker

select strtick, max(dtdate) from InsiderStocks ist
WHERE strtick in (select Ticker from InsiderPurchases
					where Filing_Date between '2021-03-09 00:00:00' and '2021-03-10 00:00:00'
					GROUP BY Ticker)
GROUP BY strtick
ORDER BY strtick

select * from InsiderPurchases

*/

/*
Insert into InsiderPurchases VALUES('20201103_200017_SQZ', 'D', '2020-11-03 20:00:17', '2020-11-03', 'SQZ', 'Sqz Biotechnologies Co', 'Polaris Management Co. Vii L.L.C.', '10', 'P - Purchase', 16.00, 250000, 2549306, 11, 4000000)
delete from InsiderPurchases where ID in ('20210222_113426_XQJ')
*/
/*

select snp.dtdate,COUNT(pur.ticker) as InsiderPurchases,spy.decClose from snp500_test snp
JOIN InsiderPurchases pur ON pur.Ticker = snp.strTick AND CAST(pur.Filing_Date as date) = snp.dtDate
JOIN (select dtdate,decClose from snp500_test where strtick in ('spy')) spy ON spy.dtDate = snp.dtDate
GROUP BY snp.dtdate,spy.decClose
order by dtdate asc
*/