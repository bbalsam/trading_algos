use stockdb

SELECT snp.dtdate,snp.strtick,snp.decOpen,snp.decClose,snp.intVol,fst.MaxHigh
FROM snp500_test snp
CROSS APPLY
	(
		SELECT	MAX(decHigh) as MaxHigh
		FROM	snp500_test as prefst
		WHERE	prefst.strtick = snp.strtick
				AND prefst.dtDate <= snp.dtDate
	) fst
WHERE snp.strTick in ('SPY')
ORDER BY snp.strTick, snp.dtDate

