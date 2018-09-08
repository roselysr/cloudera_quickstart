-- version
sqoop version

-- Help
sqoop help
sqoop help list-tables

-- List databases
sqoop-list-databases \
	--connect jdbc:mysql://localhost:3306 \
	--username retail_user -P

-- List tables (several options)
sqoop list-tables --connect jdbc:mysql://localhost:3306/retail_db --username retail_user --password cloudera 

sqoop list-tables --connect jdbc:mysql://localhost/retail_db --username retail_user -P

sqoop-list-tables --connect jdbc:mysql://localhost/retail_db --username retail_user -P

sqoop list-tables \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user -P

-- eval: used to run SQL queries against a database
sqoop eval \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user -P \
	--query "SELECT * FROM orders LIMIT 10"

sqoop eval \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user -P \
	--query "INSERT INTO orders VALUES (100000, '2017-10-31 00:00:00.0', 100000, 'DUMMY')"

sqoop eval \
	--connect jdbc:mysql://localhost/retail_export \
	--username retail_user -P \
	--query "CREATE TABLE dummy (i int)"

sqoop eval \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user -P \
	--query "SELECT * FROM order_items limit 10"

-- Import data from table
sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user -password cloudera \
	--table order_items \
	--warehouse-dir sqoop_import

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--target-dir sqoop_import/order_items

---- faster with 1 mapper
sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--target-dir sqoop_import/order_items \
	--num-mappers 1

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--target-dir sqoop_import/order_items \
	--num-mappers 1 \
	--direct

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--target-dir sqoop_import/order_items \
	--num-mappers 1 \
	--direct \
	--delete-target-dir

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--target-dir sqoop_import/order_items \
	--num-mappers 1 \
	--direct \
	--append

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items_nopk \
	--warehouse-dir sqoop_import \
	--num-mappers  4 \
	--direct \
	--delete-target-dir \
	--split-by order_item_order_id


sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table orders \
	--warehouse-dir sqoop_import \
	--num-mappers  4 \
	--direct \
	--delete-target-dir \
	--split-by order_status

sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items_nopk \
	--warehouse-dir sqoop_import \
	--direct \
	--delete-target-dir \
	--autoreset-to-one-mapper

//sequence file format
sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items_nopk \
	--warehouse-dir sqoop_import \
	--num-mappers  4 \
	--as-sequencefile

//gzip compression
sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--warehouse-dir sqoop_import \
	--direct \
	--num-mappers  1 \
	--compress \
	--compression-codec org.apache.hadoop.io.compress.SnappyCodec


//bounday query
sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--warehouse-dir sqoop_import \
	--boundary-query 'select min(order_item_id), max(order_item_id) from order_items where order_item_id > 99999'


sqoop import \
	--connect jdbc:mysql://localhost/retail_db \
	--username retail_user --password cloudera \
	--table order_items \
	--warehouse-dir sqoop_import \
	--boundary-query 'select 1000, 172198'


//selecting columns
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table order_items \
  --columns order_item_order_id,order_item_id,order_item_subtotal \
  --warehouse-dir /user/dgadiraju/sqoop_import/retail_db \
  --num-mappers 2


//specifing query
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --target-dir sqoop_import/order_items \
  --num-mappers 2 \
  --query "select o.*, sum(oi.order_item_subtotal) order_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id and \$CONDITIONS group by o.order_id, o.order_date, o.order_customer_id, o.order_status" \
  --split-by order_id


//NULLS and delimiters: Default behavior
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table employees \
  --warehouse-dir sqoop_import
  
//Changing default delimiters and nulls (using ascii null character as separator)
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table employees \
  --warehouse-dir sqoop_import \
  --null-non-string -1 \
  --fields-terminated-by "\000" \
  --lines-terminated-by ":"


# Baseline import
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --target-dir sqoop_import/order_items \
  --num-mappers 2 \
  --query "select * from orders where \$CONDITIONS and order_date like '2013-%'" \
  --split-by order_id

# Query can be used to load data based on condition
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --target-dir sqoop_import/order_items \
  --num-mappers 2 \
  --query "select * from orders where \$CONDITIONS and order_date like '2014-01%'" \
  --split-by order_id \
  --append

# where in conjunction with table can be used to get data based up on a condition
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --target-dir sqoop_import/order_items \
  --num-mappers 2 \
  --table orders \
  --where "order_date like '2014-02%'" \
  --append

# Incremental load using arguments specific to incremental load
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --target-dir sqoop_import/order_items \
  --num-mappers 2 \
  --table orders \
  --check-column order_date \
  --incremental append \
  --last-value '2014-02-28'


// Hive import: if its command is ran multiple times it will append all data again
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table order_items \
  --num-mappers 2 \
  --hive-import \
  --hive-database sqoop_import \
  --hive-table order_items


// Hive import: now it overrides old data and add the new one.
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table order_items \
  --num-mappers 2 \
  --hive-import \
  --hive-database sqoop_import \
  --hive-table order_items \
  --hive-overwrite


  // Hive import: now it will fails if the table already exists. If it fails I have to remove  /user/cloudera/<table_name>
sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table order_items \
  --num-mappers 2 \
  --hive-import \
  --hive-database sqoop_import \
  --hive-table order_items \
  --create-hive-table


 //test --map-column-hive argument

//Import all tables
 sqoop import-all-tables \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --num-mappers 2 \
  --warehouse-dir sqoop_import \
  --autoreset-to-one-mapper


  //prepare data for export
  sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table order_items \
  --num-mappers 1 \
  --hive-import \
  --hive-database sqoop_import \
  --hive-table order_items

  sqoop import \
  --connect jdbc:mysql://localhost/retail_db \
  --username retail_user --password cloudera \
  --table orders \
  --num-mappers 1 \
  --hive-import \
  --hive-database sqoop_import \
  --hive-table orders

create table daily_revenue as
select order_date, sum(order_item_subtotal) daily_revenue
from orders join order_items on
order_id = order_item_order_id
where order_date like '2013-07%'
group by order_date;

select * from daily_revenue limit 10;
hdfs://quickstart.cloudera:8020/user/hive/warehouse/sqoop_import.db/daily_revenue

//create table in mysql
create table daily_revenue (
order_date varchar(30),
revenue float
);

//export
sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --input-fields-terminated-by "\001"


//export: especifing the columns in target table
sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --columns order_date,revenue\
  --input-fields-terminated-by "\001"


//export: run export twice having primary key

create table daily_revenue_test (
order_date varchar(30),
revenue float,
PRIMARY KEY (order_date)
);

sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue_test \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --columns order_date,revenue\
  --input-fields-terminated-by "\001"


//export: fixing the issue
//only updates
sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue_test \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --columns order_date,revenue\
  --input-fields-terminated-by "\001" \
  --update-key order_date

delete from  daily_revenue_test where revenue > 136520;

//updates and insert
sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue_test \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --columns order_date,revenue\
  --input-fields-terminated-by "\001" \
  --update-key order_date \
  --update-mode allowinsert


 //To avoid inconsistency when something goes wrong
 //if export is successfull and --clear-staging-table is not used staging table will be empty
create table daily_revenue_stage (
order_date varchar(30) primary key,
revenue float
);

  sqoop export \
  --connect jdbc:mysql://localhost/retail_export \
  --username retail_user --password cloudera \
  --table daily_revenue_test \
  --staging-table daily_revenue_stage \
  --clear-staging-table \
  --export-dir /user/hive/warehouse/sqoop_import.db/daily_revenue \
  --input-fields-terminated-by "\001"

