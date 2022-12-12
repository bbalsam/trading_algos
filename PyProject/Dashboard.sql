/*======================================================
DASHBOARD BUY
======================================================*/

SELECT strTick + ' stock price' as LookupVal,strTick,decAdjClose,*
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
where ((sto.decKFAST <= 20 and sto.decKSlow < sto.deckFast) and snp.dtDate = (select max(dtdate) from snp500_test))
		--AND snp.strTick not in ('DVA','EVRG','FFIV','FLIR','FTNT','GILD','NEM','ORCL','OXY','RMD','AMT','ANET','CMS','IFF','IPGP','LLY','MYL')
ORDER BY snp.strTick asc, snp.dtDate desc

/*Individual Performance Lookup*/
DECLARE @tempTick varchar(12)
SET @tempTick = 'PNW'

select top 252 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick = @tempTick
ORDER BY dtDate desc
/*TOP PERFORMERS*/
SELECT strTick + ' stock price' as LookupVal,strTick,decAdjClose,*
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
where ((sto.decKFAST <= 50 /*and sto.decKSlow < sto.deckFast*/) and snp.dtDate = (select max(dtdate) from snp500_test))
		AND snp.strTick IN ('TSLA','NVDA','DXCM','AMD','WST','PYPL','AMZN','AAPL','ABMD','NOW','NFLX','CDNS','REGN','ROL','NEM','EBAY','SLV','LB','SNPS','ODFL','ADBE','TSCO','TMUS','CMG','IDXX','CLX','ATVI','EQIX','MSCI','DPZ','ADSK','MSFT','TTWO','CHD')
ORDER BY snp.strTick asc, snp.dtDate desc
/**/
/*======================================================
DASHBOARD SELL
======================================================*/

SELECT strTick,sto.decKFAST,sto.decKSLOW,*
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
where /*sto.decKFAST >= 80 and*/ snp.dtDate = (select max(dtdate) from snp500_test)
		AND snp.strTick in ('EVRG','GILD','NEM','RMD','ANET','CMS','IPGP','LLY','MYL')
ORDER BY snp.strTick asc, snp.dtDate desc


/*
select top 10 * from snp500_test where dtDate > getdate()-5 order by dtDate desc
*/
/*
SELECT snp.strTick + ' stock price' as Search,
snp.ID,
sto.SNP500ID,
sto.decKFAST,
sto.decKSLOW,
snp.dtDate,
snp.decOpen,
snp.decHigh,
snp.decLow,
snp.decClose,
snp.decAdjClose,
snp.intVol,
snp.strTick,
snp.ID

FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
ORDER BY snp.strTick asc, snp.dtDate desc
*/

select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN('AAPL'/*,'AMZN','GLD','NFLX','SLV','SPY','TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL',*/'AMZN'/*,'GLD','NFLX','SLV','SPY','TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL','AMZN',*/'GLD'/*,'NFLX','SLV','SPY','TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL','AMZN','GLD',*/'NFLX'/*,'SLV','SPY','TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL','AMZN','GLD','NFLX',*/'SLV'/*,'SPY','TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL','AMZN','GLD','NFLX','SLV',*/'SPY'/*,'TSLA'
*/)
UNION
select top 50 snp.strtick,snp.dtdate,sto.deckfast,sto.deckslow,snp.decOpen,snp.decAdjClose
FROM snp500_test snp
JOIN stochastic sto ON sto.SNP500ID = snp.ID
WHERE snp.strTick IN(/*'AAPL','AMZN','GLD','NFLX','SLV','SPY',*/'TSLA'/*
*/)
ORDER BY snp.strtick,dtDate desc
	

SELECT tod.strTick,tod.TodayDate,twa.PastDate,tod.TodayClose,twa.PastClose,
		(tod.TodayClose/twa.PastClose) as PctChange
FROM
(select strtick,dtdate as TodayDate,decClose as TodayClose from snp500_test
where dtDate = '2020/08/20') tod
JOIN
(select strtick,dtdate as PastDate,decClose as PastClose from snp500_test
where dtDate = dateadd(day,-252,cast('2020/08/20' as date))) twa
ON twa.strTick = tod.strTick
GROUP BY tod.strTick,tod.TodayDate,twa.PastDate,tod.TodayClose,twa.PastClose
ORDER BY (tod.TodayClose/twa.PastClose) desc