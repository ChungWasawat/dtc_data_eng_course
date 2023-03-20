from decimal import Decimal
from datetime import datetime
import faust

class GREENRide(faust.Record, validation=True):
    vendor_id: str
    pu_location_id: int
    do_location_id: int

# class GREENRide(faust.Record, validation=True):
#     vendor_id: str
#     lpep_pickup_datetime: datetime
#     lpep_dropoff_datetime: datetime
#     store_and_fwd_flag: str
#     rate_code_id: int
#     pu_location_id: int
#     do_location_id: int
#     passenger_count: int
#     trip_distance: Decimal
#     fare_amount: Decimal
#     extra: Decimal
#     mta_tax: Decimal
#     tip_amount: Decimal
#     tolls_amount: Decimal
#     ehail_fee: Decimal
#     improvement_surcharge: Decimal
#     total_amount: Decimal
#     payment_type: str
#     trip_type: str
#     congestion_surcharge: Decimal



