select distinct
  customers.customer_name
  , sum(COALESCE(order.order_amt, 0)) as total_2009

from customers
left join orders on customers.customer_nbr = orders.customer_nbr

where extract(year, orders.order_date)::int = 2009
group by customers.customer_name


;;;

-- creating test table
-- postgres 9.6
create table test (
  event_at timestamp
  , analyst_id varchar
  , sourcing decimal
  , writing decimal
  , job_action varchar
  , request_id varchar
  , request_at timestamp
  , job_type varchar
  , wait_time int
  , avail_analysts int
  , occ_analysts int
  , total_jobs_avail int
  , review_jobs_avail int
  , vet_jobs_avail int
  , plan_jobs_avail int
  , edit_jobs_avail int
  , source_jobs_avail int
  , write_jobs_avail int
  );
