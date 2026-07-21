select
    shipment_id,
    cast(leg_seq as integer) as leg_seq,
    mode,
    vessel_mmsi,
    origin,
    dest
from read_csv('../data/silver/shipment_legs.csv',
              header = true, all_varchar = true)
