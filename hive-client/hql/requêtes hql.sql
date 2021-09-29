--5.1	Creation d'une table simple avec Hive
 CREATE EXTERNAL TABLE IF NOT EXISTS sales (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/maria_dev/hive/sales/';
 
 
 
 
--5.2	Création d'une table partitionned

 CREATE EXTERNAL TABLE IF NOT EXISTS part_sales (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/maria_dev/hive/part.sales/'
 
 --5.2	Création d'une table partitionned et bucketéed

 CREATE EXTERNAL TABLE IF NOT EXISTS part_buck_sales (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 CLUSTERED BY(customer_id)  INTO 5 BUCKETS
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/maria_dev/hive/part.buck.sales/';
 
 
  --5.2	Création d'une table partitionned et bucketéed avro

 CREATE EXTERNAL TABLE IF NOT EXISTS part_buck_sales_avro (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 CLUSTERED BY(customer_id)  INTO 3 BUCKETS
 STORED AS AVRO
 LOCATION '/user/maria_dev/hive/part.buck.sales.avro/';
 
  CREATE EXTERNAL TABLE IF NOT EXISTS customers (
 customer_id String,
account_num String,
last_name String,
first_name String,
address String,
state_province String,
postal_code String,
country String,
region_id String,
phone String,
birthdate String,
marital_status String,
yearly_income String,
gender String,
total_children String,
num_children_at_home String,
education String,
date_accnt_opened String,
member_card String,
occupation String,
houseowner String,
num_cars_owned String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/maria_dev/hive/customers/';
 
 
 
 CREATE EXTERNAL TABLE IF NOT EXISTS buck_customer_orc (
 customer_id String,
account_num String,
last_name String,
first_name String,
address String,
state_province String,
postal_code String,
country String,
region_id String,
phone String,
birthdate String,
marital_status String,
yearly_income String,
gender String,
total_children String,
num_children_at_home String,
education String,
date_accnt_opened String,
member_card String,
occupation String,
houseowner String,
num_cars_owned String)
CLUSTERED BY(customer_id)  INTO 3 BUCKETS
STORED AS ORC
LOCATION '/user/maria_dev/hive/buck.customers.orc/';
 
 
 
 
--5.3	 Insert table dans une table de partition 
 INSERT INTO part_sales(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_sales ,
 store_cost ,
 unit_sales ,
 store_id from sales
 );
--5.4	  Selectionner des tables 
 select count(*) from sales_raw where store_id='1' ;
 ==>le temps d'exécution est du 6 s
 
 select count(*) from sales_partition where store_id='1' ;
 ==>Le temps d'exécusion est de 2 s

--5.5	Création d'une table ORC

CREATE EXTERNAL TABLE IF NOT EXISTS sales_orc (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 STORED AS ORC ;
 
 alter table sales_orc set LOCATION '/user/maria_dev/hive/sales.orc';

--5.6	Insertion dans une table ORC
  INSERT INTO sales_orc(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_id,
 store_sales ,
 store_cost ,
 unit_sales
 from sales
 ) ;
 
--5.7	Création d'une table de partition sous format ORC
CREATE EXTERNAL TABLE IF NOT EXISTS sales_part_orc (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 STORED AS ORC
 LOCATION  '/user/maria_dev/hive/part.sales.orc';
 
 
--5.8	 Insertion dans la table sales_partition_orc
  INSERT INTO sales_part_orc(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_sales ,
 store_cost ,
 unit_sales ,
 store_id from sales_orc
 );
--5.9	Création d'une table dans le shéma est identique à une table déjà existante
 CREATE TABLE schema_copy like sales ;
 
--5.10	 Création une table stores_raw
 
CREATE EXTERNAL TABLE IF NOT EXISTS stores (
store_id String,
store_type String,
region_id String,
store_name String,
store_number String,
store_street_address String,
store_city String,
store_state String,
store_postal_code String,
store_country String,
store_manager String,
store_phone String,
store_fax String,
first_opened_date String,
last_remodel_date String,
store_sqft String,
grocery_sqft String,
frozen_sqft String,
meat_sqft String,
coffee_bar String,
video_store String,
salad_bar String,
prepared_food String,
florist String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION  '/user/maria_dev/hive/stores';
 
5.11	Faire une joiture entre sales_raw et stores_raw
select* from sales_raw join stores_raw on (sales_raw.store_id = stores_raw.store_id) limit 5;


select last_name, first_name, sum(store_sales * unit_sales) 
from sales inner join customers
on sales.customer_id = customers.customer_id
group by last_name, first_name ;


select last_name, first_name, sum(store_sales * unit_sales) from part_buck_sales sales inner join buck_customer_orc customers on sales.customer_id = customers.customer_id group by last_name, first_name having sum(store_sales * unit_sales) > 1000;
