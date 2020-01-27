### Part 1

- 34% of analysts had average writing and sourcing scores of `0`
```
  select distinct
    analyst_id
    , avg(sourcing)
    , count(distinct event_id)
  from wonder_cleaned
  group by 1
```

- 38% of all requests were worked on by analysts that scores of `0` for writing and sourcing
```
  with zeroes as (
    select distinct
      analyst_id
      , avg(writing) avg_write_score
      , avg(sourcing) avg_source_score
      , count(distinct event_id)
    from wonder_cleaned
    group by 1
  )
  select
    count(distinct case when z.avg_source_score = 0 then request_id else null end) as requests_worked_by_zeros
    , count( distinct request_id) requests_worked
  from wonder_cleaned wc
  left join zeroes z on wc.analyst_id = z.analyst_id
```

**From the above points:**
Scoring may have a less significant impact on analysts' ability to complete reports

- Almost half of all wait time (43%) was waiting for `sourcing` alone; this indicates that sourcing is the biggest bottleneck for the business
```
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
```

- Comparing the average available jobs to the average available analysts indicates how well staffed the business is
- Looking at the query results, most of the time the amount of available analysts amounts to less than half of the count of available jobs across all hours of the available data; this may indicate that more staff is needed, or that a wait time bottleneck is preventing analysts from addressing available tasks.
```
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
```

#### Thoughts on valuable metrics
The metrics above are relatively simple and point mostly towards process and potentially indicates what sorts of analysts comprise the pool.

Given additional data sources, it would make sense to start considering how this data can be augmented for better insights and for stakeholder use.

At this point, it would be most important to speak with stakeholders on how they are currently using this data, and what their goals are. By understanding stakeholder goals then the metrics are more meaningful rather than throwing out any and all possible metrics.

The risk of creating any and all metrics that are possible from the dataset -- which in this case would mostly be process flow related -- is overwhelming a stakeholder with a bunch of information that may be mostly noise. At the early stages, it's more valuable to give simple and actionable insights rather than tossing out any metric that has potential to be significant.

The metrics above are what I perceived as most actionable by a stakeholder who is in a position to change how work is being distributed among analysts, and how analysts are being hired/contracted.

### Part 2
#### Thought process
**base model**
The base model should accomplish the task of providing a clean and untransformed version of the source data. Here, because there is no unique key, one is created. The data is otherwise unchanged.
```
create table wonder_log as (
	select md5(event_at||job_action||analyst_id || request_id ) as event_id
		, *
	from wonder___assignment_log
)
```
**dbt**
- Because this is events data, it makes a lot of sense to start looking at how often events are occurring and tying them to a timeline. The granularity of the timestamps is relatively high, so for summary purposes I would bin event counts to the hour.
  - The best model to use for this would be a recursive lag model to determine event duration.
  - Once the recursive model is completed, it would be joined to a spine to bin events and determine when events are occurring and find the most event concentrated time periods.

**visualisation tool (eg, looker)**
- Once the event model (described above) is completed, then it should be fed into a visualisation tool like looker, where it can be broken apart based on stakeholder interests.
- It makes the most sense to synthesize a summary dashboard for stakeholder use. For operational day to day purposes, a periodic summary board makes the most sense. For an executive level stakeholder, the data can be summarized over a long period in tabular form, and with plots of different metrics over time -- eg average daily wait time over the last 30 days.
- Because this is events data, it would make the most sense that end users are interested in ways to optimize process, so any visualisation would be focused on surfacing inefficiences.

### Part 3
```
/* corrected query */
select distinct
  customers.customer_name
  , customers.customer_nbr
  , sum(COALESCE(order.order_amt, 0)) as total_2009

from customers
left join orders on customers.customer_nbr = orders.customer_nbr

where extract('year', orders.order_date)::int = 2009
group by customers.customer_name
```
**Changes:**
- add `distinct` to ensure each customer is only returned once
  - add `customer_nbr` to accomodate cases of identical names
- `left outer join` changed to `left join` for easier reading
- remove parenthesis around join keys
- `where` clause updated to fix format issue with provided string and amend logic
  - the initial logic would return all years greater than `2009`
  - the format for the string should be `'yyyy-mm-dd'`; assuming postgres is the dialect in use

**Additional QA:**
- if there are multiple null values for `customer_name` then the total order amounts of all those customer that have a `customer_nbr` that is valid would be grouped together
- rather than a customer name or number, there should be a unique customer id based on attributes that would define a row as unique in the customers table
