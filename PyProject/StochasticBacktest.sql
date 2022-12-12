
SELECT *
FROM	(
		SELECT	com.strTick,com.dtDate,com.decAdjClose,--com.MA05,MA10,MA20,MA50,
				pctChgfwd,
				--pctChgbak,
				KFAST,
				AVG(com.KFAST) OVER (ORDER BY com.strtick asc, com.dtDate ASC ROWS 2 PRECEDING) AS KSLOW
		FROM
					(SELECT	strtick,
							dtDate,
							decAdjClose,
							CAST(LEAD(decAdjClose,10) OVER (ORDER BY strtick asc, dtDate ASC) / LEAD(decOpen,1) OVER (ORDER BY strtick asc, dtDate ASC) as decimal(10,3)) as pctChgfwd,
							--CAST(decAdjClose / LAG(decOpen,14) OVER (ORDER BY strtick asc, dtDate ASC) as decimal(10,3)) as pctChgbak,
							--AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 4 PRECEDING) AS MA05,
							--AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 9 PRECEDING) AS MA10,
							--AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 19 PRECEDING) AS MA20,
							--AVG(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 49 PRECEDING) AS MA50,
							--MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS L14,
							--MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) AS H14,
							CASE
							WHEN MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING) = MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)
							THEN NULL
							ELSE
							(100 * (decAdjClose-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))) / (MAX(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING)-MIN(decAdjClose) OVER (ORDER BY strtick asc, dtDate ASC ROWS 13 PRECEDING))
							END as KFAST
					FROM	snp500_test
					WHERE	dtDate >= DATEADD(DAY,-10000,GETDATE()) and dtDate <= DATEADD(DAY,-20,GETDATE())
					) com
				) val
WHERE	--strTick = 'GLD' --and dtDate = (select MAX(dtdate) from snp500_test)
		--AND
		(val.KFAST < 20 AND val.KSLOW < val.KFAST)
