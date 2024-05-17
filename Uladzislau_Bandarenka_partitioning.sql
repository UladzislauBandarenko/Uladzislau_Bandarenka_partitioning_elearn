-- in the sales_analysis database, create a table named sales_data. 
-- for the purpose of this assignment, choose the "range partitioning" approach based on the sale_date to partition the data by month.
create table sales_data(
    sale_id integer,
    product_id integer not null,
    region_id integer not null,
    salesperson_id integer not null,
    sale_amount numeric not null,
    sale_date date not null ,
    primary key (sale_id, sale_date)
) partition by range (sale_date);

-- create partitions for the past 12 months. 
-- each partition should be named in the sales_data_yyyy_mm format, where yyyy is the year and mm is the month.

create table sales_data_2023_01 partition of sales_data
    for values from ('2023-01-01') to ('2023-02-01');

create table sales_data_2023_02 partition of sales_data
    for values from ('2023-02-01') to ('2023-03-01');

create table sales_data_2023_03 partition of sales_data
    for values from ('2023-03-01') to ('2023-04-01');

create table sales_data_2023_04 partition of sales_data
    for values from ('2023-04-01') to ('2023-05-01');

create table sales_data_2023_05 partition of sales_data
    for values from ('2023-05-01') to ('2023-06-01');

create table sales_data_2023_06 partition of sales_data
    for values from ('2023-06-01') to ('2023-07-01');

create table sales_data_2023_07 partition of sales_data
    for values from ('2023-07-01') to ('2023-08-01');

create table sales_data_2023_08 partition of sales_data
    for values from ('2023-08-01') to ('2023-09-01');

create table sales_data_2023_09 partition of sales_data
    for values from ('2023-09-01') to ('2023-10-01');

create table sales_data_2023_10 partition of sales_data
    for values from ('2023-10-01') to ('2023-11-01');

create table sales_data_2023_11 partition of sales_data
    for values from ('2023-11-01') to ('2023-12-01');

create table sales_data_2023_12 partition of sales_data
    for values from ('2023-12-01') to ('2024-01-01');

--write a script to generate and insert synthetic data into sales_data. 
--ensure that the data gets correctly routed to the appropriate partitions. 
--the script should:
--generate at least 1000 rows of synthetic data distributed across the last 12 months.
--include a mix of product_id, region_id, and salesperson_id.

create or replace function generate_insert_data()
returns void
language plpgsql
as $$
declare
    sale_date date;
    new_sale_id integer;
begin
    for counter in 1..1000 loop
    sale_date := '2023-01-01'::date + (floor(random() * 365) * interval '1 day');
    new_sale_id := counter;

        insert into sales_data(sale_id, sale_date, salesperson_id, region_id, product_id, sale_amount)
        values (
            new_sale_id,
            sale_date,
            1 + floor(random() * 6), 
            1 + floor(random() * 10), 
            1 + floor(random() * 8),  
            40 + floor(random() * 1000)  
        );
    end loop;
end;
$$;

select generate_insert_data();

--retrieve all sales in a specific month
select 
	extract(month from sale_date) as month_sale,
	count(*) as month_total
from sales_data
group by month_sale
order by month_sale;

-- calculate the total sale_amount for each month
select 
  extract(month from sale_date) as month_sale, 
  sum(sale_amount) as total_amount
from sales_data
group by month_sale
order by month_sale;

--identify the top three salesperson_id values by sale_amount within a specific region across all partitions.

with person_sale as (
    select 
        salesperson_id,
        region_id,
        sum(sale_amount) as total_amount,
        rank() over (partition by region_id order by sum(sale_amount) desc) as person_rank
    from 
        sales_data
    group by 
        region_id, salesperson_id
)
select 
    salesperson_id,
    region_id,
    total_amount
from 
     person_sale
where 
    person_rank <= 3;

--define a maintenance task to drop partitions older than 12 months and create new partitions for the next month.
-- procedure to manage monthly partition maintenance
create or replace procedure manage_partitions()
language plpgsql
as $$
declare
    current_date date := current_date;
    last_year_date date := current_date - interval '1 year';
	partition_date_to_remove date;
    next_month_start date := date_trunc('month', current_date);
    next_month_end date := date_trunc('month', next_month_start) + interval '1 month';
	month_start date;
	month_end date;
	partition_date_to_add date;
    partition_name varchar;
    next_month_name varchar;
begin

    for counter in 0..11 loop
		partition_date_to_remove := last_year_date - (interval '1 month' * counter);
		partition_name := 'sales_data_' || to_char(partition_date_to_remove, 'yyyy_mm');
		if to_regclass(partition_name) is not null then
			execute format('drop table %i', partition_name);
			raise notice 'dropped partition: %', partition_name;
		else
			raise notice 'partition % does not exist, skipping drop.', partition_name;
		end if;
		
		partition_date_to_add = next_month_start - (interval '1 month' * counter);
		next_month_name := 'sales_data_' || to_char(partition_date_to_add, 'yyyy_mm');
		month_start = next_month_start - (interval '1 month' * counter);
		month_end = next_month_end - (interval '1 month' * counter);
		if to_regclass(next_month_name) is null then
			execute format('create table %i partition of sales_data for values from (%l) to (%l)', 
						   next_month_name, month_start, month_end);
			raise notice 'created partition: %', next_month_name;
		else
			raise notice 'partition % already exists, skipping creation.', next_month_name;
    end if;
	end loop;

end;
$$;

-- to automate the execution of this procedure, schedule it to run at the beginning of each month
-- this depends on the specific postgresql scheduling tools available, like pgagent or an external cron job

call manage_partitions();
