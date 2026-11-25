from travelradar.sources import (ais, digitraffic_locations, digitraffic_trains, entur, opensky,
                                 weather)

ALL = {s.name: s for s in (
    opensky.SOURCE,
    ais.SOURCE,
    digitraffic_trains.SOURCE,
    digitraffic_locations.SOURCE,
    entur.SOURCE,
    weather.SOURCE,
)}
