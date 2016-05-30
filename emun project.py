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


def diffrence_between_two_trips(arrival_time, list_of_times):
    """input: route_id of a certain route and arrival_time. output: the difference between current arrival time and next arrival time (of the same route)
        notice that if it is the last trip of this line that day, the function returns the diffrence between one-before-last trip and current trip"""
    # index of current arrival time
    pos = list_of_times.index(arrival_time)
    current_element = list_of_times[pos]
    if pos == len(list_of_times):
        prev_element = list_of_times[pos - 1]
        difference = current_element - prev_element
    else:
        next_element = list_of_times[pos + 1]
        difference = next_element - arrival_time
    return difference


def delay_percent_calculation(delay_in_minute,difference):
    """input: delay and difference between current arrival time and next arrival time (of the same route)- difference
    is the output of diffrence_between_two_trips function"""
    if difference == 0:
        return 0
    else:
        return delay_in_minute/difference


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
    current_date = dt.datetime.now()
    # connect to database
    # connection = pymssql.connect(server = 'MARKETERS-AD.COM', user = 'team1n', password = 'wxV20o*7', database = 'emon_team1')
    # connection = pymssql.connect(server='MARKETERS-AD.COM', user='team2n', password='J7t6p7s_', database='emon_team2')
    connection = pymssql.connect(server='MARKETERS-AD.COM', user='backend1n', password='Uktj93!6', database='emon_backend')
    c = connection.cursor(as_dict = True)
    trip_list = []

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
            total_delay = random.randrange(0, 10, 1)
            delay_type = temp_function(total_delay)
        else:
            total_dalay = row["Total_Delay"]
        new_trip = Trip(route_id, agent, stop_id, arrival_time, direction, delay_type, total_delay)
        trip_list.append(new_trip)
        row = c.fetchone()

    frequency_by_routeID = {}
    c.execute("SELECT T.route_id,ST.arrival_time FROM trips T, stop_times ST WHERE ST.trip_id = T.trip_id ORDER BY T.route_id, ST.arrival_time")
    row = c.fetchone()
    while row:
        route_id = row["route_id"]
        arrival_time = row["arrival_time"]
        if route_id not in frequency_by_routeID:
            frequency_by_routeID[route_id] = []
        frequency_by_routeID[route_id].append(arrival_time)
        row = c.fetchone()

    # sort trip_list object by their route_id
    trip_list.sort(key = lambda x: x.route_id)

    if trip_list:
        old_route_id = trip_list[0].route_id
        list_of_times = [time.strptime(x, "%H:%M:%S") for x in frequency_by_routeID[old_route_id]]
        # list_of_times = frequency_by_routeID[old_route_id]
        for t in trip_list:
            if t.route_id != old_route_id:
                list_of_times = [time.strptime(x, "%H:%M:%S") for x in frequency_by_routeID[old_route_id]]
                old_route_id = t.route_id
            # calculate planned time by substracting total_dalay from Actual_Time
            planned_time = t.time - dt.timedelta(minutes = total_delay)
            if is_debug:
                delay_percent = random.uniform(0,1)
            else:
                line_frequency = diffrence_between_two_trips(planned_time, list_of_times)
                delay_percent = delay_percent_calculation(t.total_delay, line_frequency)
            # date, agency_id, route_id, stop_id, planned_time, actual_time, delay, delay_type
            new_row = (current_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], str(t.agent), t.route_id, t.stop_id, planned_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], t.time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], t.total_delay, t.delay_type.name, delay_percent)
            try:
                c.execute("INSERT INTO Reports VALUES{0}".format(new_row))
                continue
            except pymssql.StandardError as e:
                print(e)

    connection.commit()
    x=1
