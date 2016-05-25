import pymssql
import enum
import statistics
import datetime as dt
import time
import random


class Delay(enum.Enum):
    no_delay = 0
    small_delay = 1
    large_delay = 2
    miss = 3


class Trip:
    def __init__(self, route_id, agent, stop_id, time, direction, delay_type, total_delay):
        self.route_id = route_id
        self.agent = agent
        self.stop_id = stop_id
        self.time = time
        self.direction = direction
        self.delay_type = delay_type
        self.total_delay = total_delay


def delays_classification(delay_type_str):
    if delay_type_str == 'miss':
        return Delay.miss
    elif delay_type_str == 'small_delay':
        return Delay.small_delay
    elif delay_type_str == 'large_delay':
        return Delay.large_delay
    else:
        return Delay.no_delay


def temp_function(delay_in_minutes):
    if delay_in_minutes == 0:
        return Delay.no_delay
    elif 0 < delay_in_minutes <= 5:
        return Delay.small_delay
    elif 5 < delay_in_minutes <= 7:
        return Delay.large_delay
    else:
        return Delay.miss


if __name__ == '__main__':
    is_debug = True
    # get current system time
    # current_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    current_date = dt.datetime.now()
    # connect to database
    connection = pymssql.connect(server='MARKETERS-AD.COM', user='team1n', password='wxV20o*7', database='emon_team1')
    # connection = pymssql.connect(server='MARKETERS-AD.COM', user='team2n', password='J7t6p7s_', database='emon_team2')
    c = connection.cursor(as_dict = True)
    # gtfs = []
    trip_list = []
    # c.execute('SELECT T.route_id, R.agency_id, S.stop_id, S.arrival_time, T.direction_id FROM trips T, stop_times S, routes R WHERE S.trip_id like T.trip_id AND T.route_id = R.route_id AND S.stop_id BETWEEN 12812 AND 13100')
    # row = c.fetchone()
    # x = row["route_id"]
    # while row:
    #     route_id = row["route_id"]
    #     agent = row["agency_id"]
    #     stop_id = row["stop_id"]
    #     arrival_time = time.strptime(row["arrival_time"], '%H:%M:%S')
    #     direction = row["direction_id"]
    #     new_trip = Trip(route_id, agent, stop_id, arrival_time, direction)
    #     gtfs.append(new_trip)
    #     row = c.fetchone()


    # c.execute('SELECT LineRef, OperatorRef, StopRef, Actual_Arrival, DirectionRef, Total_Delay, Delay_type FROM siri_feeds WHERE Actual_Arrival < GETDATE()')

    c.execute('SELECT LineRef, OperatorRef, StopRef, Actual_Arrival, DirectionRef FROM siri_feeds WHERE Actual_Arrival < GETDATE()')
    row = c.fetchone()
    while row:
        route_id = row["LineRef"]
        agent = row["OperatorRef"]
        stop_id = row["StopRef"]
        arrival_time = row["Actual_Arrival"]
        direction = row["DirectionRef"]
        if not is_debug:
            delay_type = row["Delay_type"]
        if is_debug:
            total_delay = random.randrange(0,1,10)
            delay_type = temp_function(total_delay)
        else:
            total_dalay = row["Total_Delay"]
        new_trip = Trip(route_id, agent, stop_id, arrival_time, direction, delay_type, total_delay)
        trip_list.append(new_trip)
        row = c.fetchone()


   # get list of all agents (without duplicates)
    agent_list = set([a.agent for a in trip_list])
    delays_by_agent = {a: [0, 0, 0, 0] for a in agent_list}
    delays_per_agent = {a: 0 for a in agent_list}
    # Agent, current date, #delays_type1, %delays type1, #delays_type2, %delays type2, #delays_type3, %delays type3, #delays_type4, %delays type4, mean, SD
    statistic_by_agent = []

    for t in trip_list:
        delays_by_agent[t.agent][delays_classification(t.delay_type).value] += 1
        delays_per_agent[t.agent] += t.total_delay
        if t.delay_type != 'no_delay':
            # calculate planned time by substracting total_dalay from Actual_Time
            planned_time = t.time - dt.timedelta(minutes=total_delay)
            # date, agency_id, route_id, stop_id, planned_time, actual_time, delay, delay_type
            new_row = (current_date, t.agent, t.route_id, t.stop_id, planned_time, t.time, t.total_delay, t.delay_type.name)
            c.execute("INSERT INTO Reports VALUES" %new_row)
            x=1


    for i in agent_list:
        all_trips = 0
        for j in range(4):
            all_trips += sum(delays_by_agent[i][j])
        # mean =
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
        # statistic_by_agent[i][10] = #mean
        # statistic_by_agent[i][11] = #SD
