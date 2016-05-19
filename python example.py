import pymssql
from enum import Enum
import statistics
import datetime as dt
import time


class Delay(Enum):
    no_delay = 0
    small_delay = 1
    large_delay = 2
    miss = 3


class Trip:
    def __init__(self, route_id, agent, stop_id, time, direction):
        self.route_id = route_id
        self.agent = agent
        self.stop_id = stop_id
        self.time = time
        self.direction = direction


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
    # get current system time
    current_date = dt.datetime.now()
    # connect to database
    connection = pymssql.connect(server='MARKETERS-AD.COM', user='team1n', password='wxV20o*7', database='emon_team1')
    c = connection.cursor()
    gtfs = []
    siri = []
    c.execute('SELECT T.route_id, R.agency_id, S.stop_id, S.arrival_time, T.direction_id FROM trips T, stop_times S, routes R WHERE S.trip_id like T.trip_id AND T.route_id = R.route_id AND S.stop_id BETWEEN 12812 AND 13100')
    row = c.fetchone()
    while row:
        route_id = row[0]
        agent = row[1]
        stop_id = row[2]
        arrival_time = time.strptime(row[3], '%H:%M:%S')
        direction = row[4]
        new_trip = Trip(route_id, agent, stop_id, arrival_time, direction)
        gtfs.append(new_trip)
        row = c.fetchone()

    c.execute('SELECT LineRef, OperatorRef, StopRef, Actual_Arrival, DirectionRef FROM siri_feeds WHERE Actual_Arrival < GETDATE()')
    row = c.fetchone()
    while row:
        route_id = row[0]
        agent = row[1]
        stop_id = row[2]
        arrival_time = time.strftime(row[3], '%H:%M:%S')
        direction = row[4]
        new_trip = Trip(route_id, agent, stop_id, arrival_time, direction)
        siri.append(new_trip)
        row = c.fetchone()


   # get list of all agents (without duplicates)
    agent_list = set([a.agent for a in siri])
    delays_by_agent = {a: [0,0,0,0] for a in agent_list}
    # date, trip_id, agency_id, route_id, stop_id, planned_time, actual_time, delay, delay_type
    delays_report = []
    # Agent, current date, #delays_type1, %delays type1, #delays_type2, %delays type2, #delays_type3, %delays type3, #delays_type4, %delays type4, mean, SD
    statistic_by_agent = []

    for g, s in gtfs,siri:
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

    for i in agent_list:
        all_trips = 0
        for j in range(4):
            all_trips += sum(delays_by_agent[i][j])
        statistic_by_agent[i][0] = i
        statistic_by_agent[i][1] = current_date
        statistic_by_agent[i][2] = sum(delays_by_agent[i][0])
        statistic_by_agent[i][3] = sum(delays_by_agent[i][0]) / all_trips
        statistic_by_agent[i][4] = sum(delays_by_agent[i][1])
        statistic_by_agent[i][5] = sum(delays_by_agent[i][1]) / all_trips
        statistic_by_agent[i][6] = sum(delays_by_agent[i][2])
        statistic_by_agent[i][7] = sum(delays_by_agent[i][3]) / all_trips
        statistic_by_agent[i][8] = sum(delays_by_agent[i][4])
        statistic_by_agent[i][9] = sum(delays_by_agent[i][4]) / all_trips
        statistic_by_agent[i][10] = #mean 
        statistic_by_agent[i][11] = #SD
