use stockdb
/**/
INSERT INTO snp500_test (dtDate,decOpen,decHigh,decLow,decClose,decAdjClose,intVol,strTick)

SELECT
	'2020-08-03' as dtDate,
	432.799988 as decOpen,
	446.549988 as decHigh,
	431.570007 as decLow,
	435.750000 as decClose,
	434.965759 as decAdjClose,
	77037800 as intVol,
	'AAPL' as strTick

