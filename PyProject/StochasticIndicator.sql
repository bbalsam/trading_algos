
/*======================================================
STOCHASTIC FAST INDICATOR %K

How the Stochastic Momentum Oscillator Works:
Developed in the late 1950s, the stochastic momentum oscillator 
is used to compare where a security's price closed relative to 
its price range over a given period of time—usually 14 days. It 
is calculated using the following formula:
	  
%K= (100∗(CP−L14)) / (H14−L14)
​	 
where:
CP	= Most recent closing price
L14	= Low of the 14 previous trading sessions
H14	= Highest price traded during the same 14-day period
​	
A %K result of 80 is interpreted to mean that the price of the 
security closed above 80% of all prior closing prices that have 
occurred over the past 14 days. The main assumption is that a 
security's price will trade at the top of the range in a major 
uptrend. A three-period moving average of the %K called %D is 
usually included to act as a signal line. Transaction signals 
are usually made when the %K crosses through the %D.

Generally, a period of 14 days is used in the above calculation, 
but this period is often modified by traders to make this 
indicator more or less sensitive to movements in the price of 
the underlying asset.

The result obtained from applying the formula above is known as 
the fast stochastic. Some traders find that this indicator is too 
responsive to price changes, which ultimately leads to being taken 
out of positions prematurely. To solve this problem, the slow 
stochastic was invented by applying a three-period moving average 
to the %K of the fast calculation. Taking a three-period moving 
average of the fast stochastic's %K has proved to be an effective 
way to increase the quality of transaction signals; it also reduces 
the number of false crossovers. After the first moving average is 
applied to the fast stochastic's %K, an additional three-period 
moving average is then applied—making what is known as the slow 
stochastic's %D. Close inspection will reveal that the %K of the 
slow stochastic is the same as the %D (signal line) on the fast 
stochastic.

The Bottom Line

An easy way to remember the difference between the two technical 
indicators is to think of the fast stochastic as a sports car and 
the slow stochastic as a limousine. Like a sports car, the fast 
stochastic is agile and changes direction very quickly in response 
to sudden changes. The slow stochastic takes a little more time to 
change direction but promises a very smooth ride.

Mathematically, the two oscillators are nearly the same except that 
the slow stochastic's %K is created by taking a three-period average 
of the fast stochastic's %K. Taking a three-period moving average of 
each %K will result in the line that is used for a signal.

======================================================*/
use stockdb
/*
======================================================
FAST STOCHASTIC FINAL
======================================================

DECLARE @TickTemp	varchar(12)
DECLARE @lastDate	date
DECLARE @Kfast		DECIMAL(6, 3)
DECLARE @SNP500ID	varchar(16)
SET		@TickTemp = 'GLD'
SET		@lastDate = (SELECT		MAX(snp.dtDate)
					FROM		snp500_test snp
					LEFT JOIN	Stochastic sto ON sto.SNP500ID = snp.ID
					WHERE		strTick = @TickTemp
								AND sto.SNP500ID IS NULL)
SET		@SNP500ID = (SELECT	STO.strTick + '_' + REPLACE(cast(@lastDate as varchar),'-','')
					FROM
					(SELECT	LFT.strTick,
							MAX(LFT.dtDate) AS Date,
							MAX(LFT.decAdjClose) AS H,
							MIN(LFT.decAdjClose) AS L,
							SUM	(
								CASE
									WHEN dtDate = @lastDate THEN LFT.decAdjClose
									ELSE 0
								END
								) AS CP
					FROM	(
							SELECT	TOP 14 *
							FROM	snp500_test
							WHERE	strTick = @TickTemp
									AND dtDate <= @lastDate
							ORDER BY
									dtDate desc
							) LFT
					GROUP BY	LFT.strTick
					) STO)
SET @Kfast = (SELECT	(100*(CP-L)) / (H-L)
			FROM
			(SELECT	LFT.strTick,
					MAX(LFT.dtDate) AS Date,
					MAX(LFT.decAdjClose) AS H,
					MIN(LFT.decAdjClose) AS L,
					SUM	(
						CASE
							WHEN dtDate = @lastDate
							THEN LFT.decAdjClose
							ELSE 0
						END
						) AS CP
			FROM	(
					SELECT	TOP 14 *
					FROM	snp500_test
					WHERE	strTick = @TickTemp
							AND dtDate <= @lastDate
					ORDER BY
							dtDate desc
					) LFT
			GROUP BY	LFT.strTick
			) STO)

INSERT INTO Stochastic (SNP500ID,decKFAST) VALUES (@SNP500ID,@Kfast)
*/
/*
======================================================
KFAST Version 1 DRAFT
======================================================
SELECT	STO.strTick + '_' + REPLACE(cast(@lastDate as varchar),'-','') as SNP500ID,
		(100*(CP-L)) / (H-L) AS KFAST
FROM
(SELECT	LFT.strTick,
		MAX(LFT.dtDate) AS Date,
		MAX(LFT.decAdjClose) AS H,
		MIN(LFT.decAdjClose) AS L,
		SUM	(
			CASE
				WHEN dtDate = @lastDate THEN LFT.decAdjClose
				ELSE 0
			END
			) AS CP
FROM	(
		SELECT	TOP 14 *
		FROM	snp500_test
		WHERE	strTick = @TickTemp
				AND dtDate <= @lastDate
		ORDER BY
				dtDate desc
		) LFT
GROUP BY	LFT.strTick
) STO
======================================================
KFAST END
======================================================
*/

/*
======================================================
Create Stochastic table
======================================================

Create table Stochastic
	(
	ID int IDENTITY(1,1) PRIMARY KEY,
	SNP500ID varchar(16) NOT NULL UNIQUE,
    decKFAST DECIMAL(6, 3),
    decKSLOW DECIMAL(6, 3)
	)

	DROP TABLE Stochastic

======================================================
Create Stochastic table END
======================================================
*/

/*
======================================================
STOCHASTIC SLOW START
======================================================

DECLARE @kSlow  DECIMAL(6, 3)
DECLARE @SNP500ID	varchar(16)

SET @kSlow = (SELECT AVG(thr.decKFAST) as KSLOW
			FROM
			(SELECT TOP 3 sto.decKFAST
			FROM	stochastic sto
			JOIN	snp500_test snp ON snp.ID = sto.snp500ID
			WHERE	snp.strTick like 'GLD' and sto.deckslow is null
			GROUP BY snp.dtdate,sto.decKFAST,sto.snp500ID
			HAVING	snp.dtdate <= max(snp.dtdate)
			ORDER BY sto.snp500ID DESC
			) thr)

SET @SNP500ID = (SELECT thr.ID as ID
				FROM
				(SELECT TOP 1 snp.ID
				FROM	stochastic sto
				JOIN	snp500_test snp ON snp.ID = sto.snp500ID
				WHERE	snp.strTick like 'GLD' and sto.deckslow is null
				GROUP BY snp.dtdate,sto.decKFAST,snp.ID
				HAVING	snp.dtdate = max(snp.dtdate)
				ORDER BY snp.ID DESC
				) thr)

UPDATE stochastic SET decKSlow = @kSlow where snp500ID = @SNP500ID


======================================================
STOCHASTIC SLOW END
======================================================
*/
/*
SELECT COUNT(*)
FROM
	(SELECT TOP 3 sto.decKFAST
	FROM	stochastic sto
	JOIN	snp500_test snp ON snp.ID = sto.snp500ID
	WHERE	snp.strTick like 'GLD' and sto.deckslow is null
	GROUP BY snp.dtdate,sto.decKFAST,sto.snp500ID
	HAVING	snp.dtdate <= max(snp.dtdate)
	ORDER BY sto.snp500ID DESC
	) thr
*/


/*
======================================================
STOCHASTIC QUERIES
======================================================
*/
/*
SELECT		count(*)
					FROM		snp500_test snp
					LEFT JOIN	Stochastic sto ON sto.SNP500ID = snp.ID
					WHERE		strTick = 'GLD'
								AND sto.SNP500ID IS NULL
								AND snp.dtDate >= getdate() -100

*/
/*
use stockdb

DECLARE @fDate	date
SET @fDate = '2020-08-13'

Select LEFT(sto.SNP500ID,CHARINDEX('_',sto.SNP500ID)-1) + ' stock price' as Search,*
from stochastic sto
JOIN snp500_test snp ON snp.ID = sto.snp500ID
where snp.dtDate >= dateadd(day,-30,@fDate)
order by snp.ID asc,snp.dtDate desc


select * from snp500_test order by dtDate desc
*/