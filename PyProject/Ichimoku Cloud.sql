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
IF OBJECT_ID('tempdb..#ICHIMOKU') IS NOT NULL DROP TABLE #ICHIMOKU 
SELECT --TOP 1000
		dtdate,
		strTick,
		decAdjClose,
		decOpen,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2 as CL,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2 as BL,
		(((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 8 PRECEDING))/2)+((min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 25 PRECEDING))/2))/2 as SPAN_A,
		(min(decLow) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING) + max(decHigh) OVER (partition by strTick order by dtdate ASC ROWS 51 PRECEDING))/2 as SPAN_B
INTO #ICHIMOKU
FROM snp500_test
--WHERE dtdate >= dateadd(day,-290,getdate())
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
from #ICHIMOKU
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
IF(AND(LAGSpanA26<decAdjClose,LAGSpanB26<decAdjClose),1,0)
Buy 2 logic:
IF(AND(OR(CL>BL,CL=BL),CL-1<BL-1),1,0)
		LAG(decAdjClose,26) OVER (partition by strTick order by dtdate ASC) as LAGClose26,
		CL,
		BL,
		decOpen,
		LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC) as LAGSpanA26,
		LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC) as LAGSpanB26
================================================*/
/*
INSERT INTO Ichimoku (strTick,dtBuyDate,decBuyCost)
SELECT strTick,BuyDate,BuyPrice
FROM
(select StrTick,
		CASE	WHEN	(LAG(SPAN_A,26) OVER (partition by strTick order by dtdate ASC) < decAdjClose)
				AND		(LAG(SPAN_B,26) OVER (partition by strTick order by dtdate ASC) < decAdjClose)
				THEN	1
				ELSE	0
		END AS BUY1,
		CASE	WHEN	(CL > BL OR CL = BL)
				AND		(LAG(CL,1) OVER (partition by strTick order by dtdate ASC)
				< LAG(BL,1) OVER (partition by strTick order by dtdate ASC))
				THEN	1
				ELSE	0
		END AS BUY2,
		dtdate as BuyDate,
		LEAD(decOpen,1) OVER (partition by strTick Order by dtDate asc) as BuyPrice

from #ICHIMOKU
) IBT
WHERE IBT.buy1 = 1 AND IBT.buy2 = 1
*/
/*================================================
Ichimoku Backtest STEP 2 update SELL
Sell logic:
IF(decAdjClose<LAGClose26,1,0)
================================================*/
IF OBJECT_ID('tempdb..#ICHIMOKUSELL') IS NOT NULL DROP TABLE #ICHIMOKUSELL 
select * INTO #ICHIMOKUSELL
FROM 
(select StrTick,
		CASE	WHEN	decAdjClose < (LAG(decAdjClose,26) OVER (partition by strTick order by dtdate ASC))
				THEN	1
				ELSE	0
		END AS SELL1,
		dtdate as SellDate,
		LEAD(decOpen,1) OVER (partition by strTick Order by dtDate asc) as SellPrice
from #ICHIMOKU) SL
where SL.SELL1 = 1

drop table IchimokuSELL

select top 1 strtick,min(dtbuyDate) as MinBuyDate
from Ichimoku
WHERE bitOpenPosition is null
Group by strtick

Begin Tran
UPDATE Ichimoku set dtSellDate = (select top 1 SellDate
from #ICHIMOKUSELL IMS
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
from #ICHIMOKUSELL IMS
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


select * from Ichimoku where bitOpenPosition is null