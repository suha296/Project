import pymssql
from enum import Enum
import statistics
import datetime as dt


class Delay(Enum):
    no_delay = 0
    small_delay = 1
    large_delay = 2
    miss = 3


class Trip:
    def __init__(self, trip_id, date, time, agent=None):
        self.trip_id = trip_id
        self.date = date
        self.time = time
        self.agent = agent
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
    current_date = dt.datetime.now()
    connection = pymssql.connect(server='MARKETERS-AD.COM', user='team1n', password='wxV20o*7', database='emon_team1')
    c = connection.cursor()
    dict_gtfs = {}
    dict_siri = {}
    c.execute('SELECT trip_id, arrival_time FROM stop_time')
    c.execute('SELECT ArrivalTime, DatedVehicleJumetRef, OperatorRef FROM siri WHERE ActualDate < ' + current_date)

    gtfs = []
    siri = []
    # get list of all agents (without duplicates)
    agent_list = set([a.agent for a in siri])
    delays_by_agent = {a: [0,0,0,0] for a in agent_list}
    # date, trip_id, agency_id, route_id, stop_id, planned_time, actual_time, delay, delay_type
    delays_report = []

    for g,s in gtfs,siri:
        if g.trip_id == s.trip_id:
            delay_time = s.time - g.time
            if delay_time < 0:
                delay_time = 0
            delay_type = delays_classification(delay_time)
            agent = s.agent
            delays_by_agent[agent][delay_type] += 1
            if delay_type != Delay.no_delay:
                new_row = [current_date, g.trip_id, agent, g.time, s.time, delay_time, delay_type]
                delays_report.append(new_row)