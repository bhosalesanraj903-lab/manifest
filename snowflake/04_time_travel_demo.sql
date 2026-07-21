-- R13: Time Travel demo — "what did the exception queue look like before the
-- last load?" and instant recovery from a bad load.
select count(*) from manifest.silver.exception_queue;                 -- now
select count(*) from manifest.silver.exception_queue at(offset => -60*60); -- 1h ago

-- Oops-recovery: restore the table to the pre-load state
-- create or replace table manifest.silver.exception_queue clone
--   manifest.silver.exception_queue at(offset => -60*60);
