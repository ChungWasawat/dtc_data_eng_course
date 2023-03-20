import faust

class TaxiRide(faust.Record, validation=True):
    vehicle_id: str
    pu_location_id: int
    do_location_id: int