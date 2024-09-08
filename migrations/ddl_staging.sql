--clear
DROP TABLE IF EXISTS staging.user_activity_log;
DROP TABLE IF EXISTS staging.user_order_log;
DROP TABLE IF EXISTS staging.customer_research;

CREATE TABLE staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);

--user activity log
CREATE TABLE staging.user_activity_log(
   uniq_id varchar(32) NOT NULL,
   date_time          TIMESTAMP ,
   action_id             BIGINT ,
   customer_id             BIGINT ,
   quantity             BIGINT ,
   CONSTRAINT user_activity_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX ua1 ON staging.user_activity_log (customer_id);

--customer research
CREATE TABLE staging.customer_research(   
   date_id          TIMESTAMP ,
   category_id integer,
   geo_id   integer,
   sales_qty    integer,
   sales_amt numeric(14,2)   
);
CREATE INDEX cr1 ON staging.customer_research (category_id);