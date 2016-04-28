import pymssql
from enum import Enum


class Delay(Enum):
    no_delay = 1
    small_delay = 2
    large_delay = 3
    miss = 4


class Trip:
    def __init__(self, trip_id, date, time, operator, is_real_time):
        self.trip_id = trip_id
        self.date = date
        self.time = time
        self.operator=operator
        # self.is_real_time = is_real_time



def delays_classification(delay_time):
    if delay_time <= 0.5:
        return Delay.no_delay
    elif 0.5 < delay_time <= 5:
        return Delay.small_delay
    # else:
    # query to find the next bus

    #elif 5 < delay_time <= frequency:
    #    return Delay.large_delay

    else:
        return Delay.miss

if __name__ == '__main__':
    connection = pymssql.connect(server='MARKETERS-AD.COM', user='team1n', password='wxV20o*7', database='emon_team1')
    db_cursor = connection.cursor()
    db_cursor.execute('SELECT arrival_time, trip_id FROM Stops_time_gtfs', dict_gtfs)
    db_cursor.execute('SELECT ArrivalTime, DatedVehicleJumetRef FROM SIRI', dict_siri)

    gtfs = []
    siri = []
    sum = 0
    count = 0

    for g,s in gtfs,siri:
        if g.trip_id == s.trip_id and g.date == s.date:
            delay_time = s.time - g.time
            delay_type = delays_classification(delay_time)

            if (delay_time<0):
                delay_time=0
            sum = sum + delay_time
            count = count+1




