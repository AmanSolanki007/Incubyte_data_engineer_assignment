#--------------------------------------------------------------------------------------------------------------------------------------
#Description	: Loading of txt pipe delimiter file into hive stage table and load into respective country table.
#Owner		: Aman Solanki
#Created Date	: 23-May-2021 	
#--------------------------------------------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------------------------------------------
#Observations
#--------------------------------------------------------------------------------------------------------------------------------------
For this assigment fist i have created the file with name customer_20210523_111025.txt. I have placed this file on linux machine on local.

1. No Postal Code is available into data.
2. In Schema you have mentioned the date as datatype. but after loading it will show null into table because of data type mismatch.
For resolution i chave changed the datatype as string but if we want to take date format we can cast it.
3. We have selected only those columns which is required for table_india. After filter we will get only one record for insert into table_india table.
4. We have difference in header and schema peovide for staging table.
5. During the insert it will start map reduce task becasue we have filter condition in select query.


#--------------------------------------------------------------------------------------------------------------------------------------
#Solution Step By Step
#--------------------------------------------------------------------------------------------------------------------------------------
create external table customer_stg1(Customer_Name varchar(255),Customer_ID varchar(18),Customer_Open_Date string,Last_Consulted_Date string,Vaccination_Type varchar(5),Doctor_Consulted varchar(255),State varchar(5),Country varchar(5),Date_of_Birth string,Active_customer varchar(1)) ROW FORMAT DELIMITED fields terminated by '|' stored as textfile ;

load data local inpath '/customer_20210523_111025.txt' into table customer_stg1;

create external table table_india(Name varchar(255),Cust_I varchar(18),Open_Dt string,Consul_Dt string,VAC_ID varchar(5),DR_Name varchar(255),State varchar(5),Country varchar(5),DOB string,FLAG varchar(1)) stored as textfile ;

insert into table table_india
select Customer_Name,Customer_ID,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,
Country,Date_of_Birth,Active_customer from customer_stg1 where country='IND';

create external table table_usa(Name varchar(255),Cust_I varchar(18),Open_Dt string,Consul_Dt string,VAC_ID varchar(5),DR_Name varchar(255),State varchar(5),Country varchar(5),DOB string,FLAG varchar(1)) stored as textfile ;

insert into table table_usa
select Customer_Name,Customer_ID,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,
Country,Date_of_Birth,Active_customer from customer_stg1 where country='USA';

create external table table_phil(Name varchar(255),Cust_I varchar(18),Open_Dt string,Consul_Dt string,VAC_ID varchar(5),DR_Name varchar(255),State varchar(5),Country varchar(5),DOB string,FLAG varchar(1)) stored as textfile ;

insert into table table_phil
select Customer_Name,Customer_ID,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,
Country,Date_of_Birth,Active_customer from customer_stg1 where country='PHIL';

create external table table_NYC(Name varchar(255),Cust_I varchar(18),Open_Dt string,Consul_Dt string,VAC_ID varchar(5),DR_Name varchar(255),State varchar(5),Country varchar(5),DOB string,FLAG varchar(1)) stored as textfile ;

insert into table table_NYC
select Customer_Name,Customer_ID,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,
Country,Date_of_Birth,Active_customer from customer_stg1 where country='NYC';

create external table table_AU(Name varchar(255),Cust_I varchar(18),Open_Dt string,Consul_Dt string,VAC_ID varchar(5),DR_Name varchar(255),State varchar(5),Country varchar(5),DOB string,FLAG varchar(1)) stored as textfile ;

insert into table table_AU
select Customer_Name,Customer_ID,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted,State,
Country,Date_of_Birth,Active_customer from customer_stg1 where country='AU';
