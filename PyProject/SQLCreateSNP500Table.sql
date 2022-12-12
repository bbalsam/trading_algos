use stockdb
--CREATE TABLE snp500_test (
--/*    ID int NOT NULL UNIQUE,*/
--    dtDate date,
--    decOpen DECIMAL(10, 6),
--    decHigh DECIMAL(10, 6),
--	decLow DECIMAL(10, 6),
--	decClose DECIMAL(10, 6),
--	decAdjClose DECIMAL(10, 6),
--	intVol int,
--	strTick varchar(12),
--	ID varchar(16) NOT NULL UNIQUE
--);

--drop table snp500_test

--delete from snp500_test

select MIN(dtDate) as MinDate,MAX(dtDate) as MaxDate,strTick,count(*) from snp500_test
Group By strTick
order by strTick asc

select * from
(select len(strTick) cnt from snp500_test) bla
group by bla.cnt


select count(bla.num)
from (
select count(*) as num FROM snp500_Test
Group by strTick) bla


select * from snp500_test

/*
Create company list table
*/

--CREATE TABLE CompanyList (
--	ID int IDENTITY(1,1) PRIMARY KEY,
--	SectorListID INT,
--	strTick varchar(12),
--	strCompanyName varchar(64)
--);


--CREATE TABLE SectorList (
--	ID int IDENTITY(1,1) PRIMARY KEY,
--	strSectorName varchar(64)
--);

select * from companylist cl
JOIN sectorlist sl ON cl.SectorListID = sl.ID
where strTick in ('AAPL','GOOGL','SLV','GLD','TSLA','AMZN','F','GM')

select * from snp500_test where strTick in ('TSLA')

