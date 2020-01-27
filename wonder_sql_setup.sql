-- creating test table
-- postgres 9.6
create table wonder___assignment_log (
  event_at timestamp
  , analyst_id varchar
  , sourcing decimal
  , writing decimal
  , job_action varchar
  , request_id varchar
  , request_created_at timestamp
  , job_type varchar
  , wait_time int
  , wait_reasons varchar
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

/* table with unique event id */
create table wonder_log as (
	select md5(event_at||job_action||analyst_id || request_id ) as event_id
		, *
	from wonder___assignment_log
)

/* select average job scores by analyst_id */
select distinct
  analyst_id
--  , job_action
  , avg(sourcing)
  , count(distinct event_id)
from wonder_log
group by 1

/* jobs worked by zero scorers */
with zeroes as (
  select distinct
    analyst_id
    , avg(writing) avg_write_score
    , avg(sourcing) avg_source_score
    , count(distinct event_id)
  from wonder_log
  group by 1
)
select
  count(distinct case when z.avg_source_score = 0 then request_id else null end) as requests_worked_by_zeros
  , count( distinct request_id) requests_worked
from wonder_log wc
left join zeroes z on wc.analyst_id = z.analyst_id

/* select analysts with averages of zero and look at what type of jobs they took on */

with zeroes as (
  select distinct
    analyst_id
    , avg(writing) avg_write_score
    , avg(sourcing) avg_source_score
    , count(distinct event_id)
  from wonder_log
  group by 1
)
select distinct
  event_id
  , analyst_id
  , job_action
  , job_type
from wonder_log wc
left join zeroes z on wc.analyst_id = z.analyst_id
where z.avg_source_score = 0

/* looking at wait time by cause */
select distinct
  wait_reasons
  , sum(wait_time)
  , sum(wait_time)*100/total_time
from wonder_log
cross join (
	select sum(wait_time) as total_time
	from wonder_log
) a
group by wait_reasons, total_time
order by 2 desc

/* looking at types of jobs available over time */
select distinct
  event_at
  , avail_analysts
  , occ_analysts
  , review_jobs_avail
  , vet_jobs_avail
  , plan_jobs_avail
  , edit_jobs_avail
  , source_jobs_avail
  , write_jobs_avail
from wonder_log
order by 2

/* jobs avail avg by day */
select distinct
  date_trunc('hour', event_at)
  , avg(avail_analysts) avail_analysts
  , avg(occ_analysts) occ_analysts
  , avg(review_jobs_avail) review_jobs_avail
  , avg(vet_jobs_avail) vet_jobs_avail
  , avg(plan_jobs_avail) plan_jobs_avail
  , avg(edit_jobs_avail) edit_jobs_avail
  , avg(source_jobs_avail) source_jobs_avail
  , avg(write_jobs_avail) write_jobs_avail
from wonder_log
group by 1
order by 2
