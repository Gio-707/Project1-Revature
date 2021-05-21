-- creating project database
CREATE DATABASE project1;

USE project1;
-- creating tables
--bev branch tables
create table BevbranchA(beverage STRING, branch STRING)row format delimited fields terminated by ',';
create table BevbranchB(beverage STRING, branch STRING)row format delimited fields terminated by ',';
create table BevbranchC(beverage STRING, branch STRING)row format delimited fields terminated by ',';

--bevcount tables
create table BevcountA(beverage STRING, count INT)row format delimited fields terminated by ',';
create table BevcountB(beverage STRING, count INT)row format delimited fields terminated by ',';
create table BevcountC(beverage STRING, count INT)row format delimited fields terminated by ',';



-- loading data into tables
--bev and branch tables
load data inpath '/user/gio/project1data/Bev_BranchA.txt' into table BevBranchA;
load data inpath '/user/gio/project1data/Bev_BranchB.txt' into table BevBranchB;
load data inpath '/user/gio/project1data/Bev_BranchC.txt' into table BevBranchC;

-- bev and consume table
load data inpath '/user/gio/project1data/Bev_ConsCounta.txt' into table BevcountA;
load data inpath '/user/gio/project1data/Bev_ConsCountb.txt' into table BevcountB;
load data inpath '/user/gio/project1data/Bev_ConsCountc.txt' into table BevcountC;


-- answering Problem Scenario 1 
/*What is the total number of consumers for Branch1?
What is the number of consumers for the Branch2?
Type 1: Creating single physical table with sub queries.
Type 2: Creating multiple physical tables
"use any one type which you are comfortable"*/

--creating table with all brach 1 bevs
create table Branch1 as select * from BevBranchA where branch = 'Branch1';
insert into table Branch1 select * from BevBranchB where branch = 'Branch1';
insert into table Branch1 select * from BevBranchC where branch = 'Branch1';
select * from branch1;

-- creating table to count the bevs of branch 1
create table Branch1Count (beverage String, count int);

insert into table branch1count select bevcountA.beverage, sum(bevcounta.count) from branch1 
join bevcounta on(branch1.beverage = bevcounta.beverage) group by bevcounta.beverage;

insert into table branch1count select bevcountB.beverage, sum(bevcountB.count) from branch1 
join bevcountB on(branch1.beverage = bevcountB.beverage) group by bevcountB.beverage;

insert into table branch1count select bevcountC.beverage, sum(bevcountC.count) from branch1 
join bevcountC on(branch1.beverage = bevcountC.beverage) group by bevcountC.beverage;

-- our result for branch1 
select sum(count) from branch1count;

-- repeat process for branch 2
create table Branch2 as select * from BevBranchA where branch = 'Branch2';
insert into table Branch2 select * from BevBranchB where branch = 'Branch2';
insert into table Branch2 select * from BevBranchC where branch = 'Branch2';
 
create table Branch2Count (beverage String, count int);
select * from branch2count;
insert into table branch2count select bevcountA.beverage, sum(bevcounta.count) from branch2 
join bevcounta on(branch2.beverage = bevcounta.beverage) group by bevcounta.beverage;

insert into table branch2count select bevcountb.beverage, sum(bevcountb.count) from branch2 
join bevcountb on(branch2.beverage = bevcountb.beverage) group by bevcountb.beverage;

insert into table branch2count select bevcountc.beverage, sum(bevcountc.count) from branch2 
join bevcountc on(branch2.beverage = bevcountc.beverage) group by bevcountc.beverage;

-- result for branch 2
select sum(count) from branch2count;

/*Problem Scenario 2 
What is the most consumed beverage on Branch1
What is the least consumed beverage on Branch2*/

-- finding most consumed bevvy in branch1
select beverage, sum(count) as total from branch1count group by beverage order by total desc;

-- finding least consumed bevyy in branch2
select beverage, sum(count) as total from branch2count group by beverage order by total;


/*
Problem Scenario 3
What are the beverages available on Branch10, Branch8, and Branch1?
what are the comman beverages available in Branch4,Branch7?
*/
-- table with data from branch 10, 8, 1
create table branch1ten8bevs (beverage String, branch String);

insert into branch1ten8bevs select * from bevbrancha where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
insert into branch1ten8bevs select * from bevbranchb where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
insert into branch1ten8bevs select * from bevbranchc where branch = 'Branch10' or branch = 'Branch8' or branch = 'Branch1';
-- query to show part 1 solution
select distinct(beverage) from branch1ten8bevs order by beverage;


-- tables for beverages in 4 and 7


create table bevy7 as select * from bevbrancha where branch = 'Branch7' union all select * from bevbranchb where branch = 'Branch7' union all select * from 
   bevbranchc where branch = 'Branch7';

create table bevy4 as select * from bevbrancha where branch = 'Branch4' union all select * from bevbranchb where branch = 'Branch4' union all select * from 
   bevbranchc where branch = 'Branch4';

  -- query to show part 2 solution
select distinct(bevy7.beverage), bevy7.branch as branch7, bevy4.branch as branch4 from bevy7 inner join bevy4 on bevy7.beverage = bevy4.beverage order by bevy7.beverage; 



/*--Problem Scenario 4
--create a partition,index,View for the scenario3.*/

-- creating partition on table containing beverages in table 10, 8, 1 ie branch1ten8bevs
create table bevypart (beverage String) partitioned by (branch String);
alter table bevypart add partition(branch = 'Branch1');
alter table bevypart add partition(branch = 'Branch8');
alter table bevypart add partition(branch = 'Branch10');
insert into bevypart partition(branch) select beverage, branch as branch from branch1ten8bevs;
-- needed following to be able to load nonparitioned table into partitioned table
set hive.exec.dynamic.partition.mode=nonstrict;
show partitions bevypart;
-- can show partitions better in hdfs

-- creating a index onf shared beverages of 10 ,8 ,1 note: as speciefies type of indes, 2 kinds in hive
create index shared_bevys on table bevypart(beverage) as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
with deferred rebuild;
-- to show our new index
show index  on bevypart;


-- will create a view of query used on bevy7 and bevy4 to show drinks available in both branches
create view dem4and7Dranks as select distinct(bevy7.beverage), bevy7.branch as branch7, bevy4.branch as branch4 from 
   bevy7 inner join bevy4 on bevy7.beverage = bevy4.beverage order by bevy7.beverage;
select * from dem4and7dranks;
-- looked up and no specific command to show views in hive, but they show up in dbeaver.

/*Problem Scenario 5
Alter the table properties to add "note","comment"*/

-- will add note and comment to our partitioned TABLE 
alter table bevypart set tblproperties ('note' = 'Table Partitioned by Branch');
alter table bevypart set tblproperties ('comment' = 'This table contains beverages avaialable in branch 1, 8, or 10');
show tblproperties bevypart;


/*
Problem Scenario 6
Remove the row 5 from the output of Scenario 1
*/
-- note table branch1count must be dropped and recreated or will mess up count of branch1
-- created a table to use for removal, will drop branch1count and create new branch1count without 5th highest
-- 5th highest is special espresso at 7992, had id 44
create table rowremover as select * , ROW_NUMBER() over() as id from branch1count;
-- used this to find 5th highest
select * from rowremover order by count;
drop table branch1count;
create table branch1count as select beverage, count from rowremover where id != 44;
select * from branch1count order by count;
-- Success!


