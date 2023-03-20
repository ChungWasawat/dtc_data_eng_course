from datetime import datetime
import faust

# datetime.strptime(variable, "%Y-%m-%d %H:%M:%S"),

# class FHVRide(faust.Record, validation=True):
#     dispatching_base_num: str
#     pickup_datetime: datetime
#     dropoff_datetime: datetime
#     pu_location_id: int
#     do_location_id: int
#     sr_flag: int
#     affiliated_base_number: str


class FHVRide(faust.Record, validation=True):
    dispatching_base_num: str
    pu_location_id: int
    do_location_id: int


