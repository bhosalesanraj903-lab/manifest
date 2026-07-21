-- R13: continuously-maintained exception mart (no orchestration needed).
create or replace dynamic table manifest.gold.exception_mart
  target_lag = '15 minutes'
  warehouse = manifest_wh
as
select
    e.exception_type,
    e.probable_cause,
    e.carrier,
    count(*)                as open_exceptions,
    avg(e.age_hours)        as avg_age_h,
    max(e.age_hours)        as max_age_h,
    count_if(e.age_hours >= 96) as band2_escalations
from manifest.silver.exception_queue e
group by 1, 2, 3;
