select
    exception_id,
    shipment_id,
    carrier,
    exception_type,
    strptime(detected_at, '%Y-%m-%dT%H:%M:%SZ') as detected_at,
    cast(age_hours as double) as age_hours,
    detail,
    probable_cause
from read_csv('../data/silver/exception_queue.csv',
              header = true, all_varchar = true)
