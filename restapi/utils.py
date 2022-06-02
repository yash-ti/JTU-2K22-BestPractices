import urllib.request
from datetime import datetime
import constants
from typing import Dict, List, Tuple

def normalize(expense) -> List:
    """Normalize the expenses"""
    user_balances = expense.users.all()
    dues: Dict = {}
    for user_balance in user_balances:
        dues[user_balance.user] = dues.get(user_balance.user, 0) + user_balance.amount_lent \
                                  - user_balance.amount_owed
    dues: List[Tuple] = [(k, v) for k, v in sorted(dues.items(), key=lambda item: item[1])]
    start: int = 0
    end: int = len(dues) - 1
    balances: List = []
    while start < end:
        amount: float = min(abs(dues[start][1]), abs(dues[end][1]))
        user_balance: Dict[str, float] = {"from_user": dues[start][0].id, "to_user": dues[end][0].id, "amount": amount}
        balances.append(user_balance)
        dues[start] = (dues[start][0], dues[start][1] + amount)
        dues[end] = (dues[end][0], dues[end][1] - amount)
        if dues[start][1] == 0:
            start += 1
        else:
            end -= 1
    return balances


def sort_by_time_stamp(logs: List[str]) -> List[List[str]]:
    """ Sorts logs by their timestamp
        Returns a list of sorted logs.
    """
    data: List[List[str]] = []
    for log in logs:
        data.append(log.split(" "))
    # print(data)
    data = sorted(data, key=lambda elem: elem[1])
    return data

def response_format(raw_data: Dict[str, Dict[str, int]]) -> List[Dict]:
    """ Converts the logs into an appropriate format for the response"""
    response: List[Dict] = []
    for timestamp, data in raw_data.items():
        entry: Dict = {'timestamp': timestamp}
        logs: List = []
        data: Dict[str, int] = {k: data[k] for k in sorted(data.keys())}
        for exception, count in data.items():
            logs.append({'exception': exception, 'count': count})
        entry['logs'] = logs
        response.append(entry)
    return response

def aggregate(cleaned_logs: List[List[str]]) -> Dict[str, Dict[str, int]]:
    """ Aggregates data from the cleaned logs, returns a dict"""
    data: Dict[str, Dict[str, int]] = {}
    for log in cleaned_logs:
        [key, text] = log
        value = data.get(key, {})
        value[text] = value.get(text, 0)+1
        data[key] = value
    return data


def transform(logs: List[List[str]]) -> List[List[str]]:
    """ Transforms logs into [timestamp, text] form"""
    result: List[List[str]] = []
    for log in logs:
        [_, timestamp, text] = log
        text: str = text.rstrip()
        timestamp = datetime.utcfromtimestamp(int(int(timestamp)/1000))
        hours, minutes = timestamp.hour, timestamp.minute
        key: str = ''

        if minutes >= 45:
            if hours == 23:
                key = "{:02d}:45-00:00".format(hours)
            else:
                key = "{:02d}:45-{:02d}:00".format(hours, hours+1)
        elif minutes >= 30:
            key = "{:02d}:30-{:02d}:45".format(hours, hours)
        elif minutes >= 15:
            key = "{:02d}:15-{:02d}:30".format(hours, hours)
        else:
            key = "{:02d}:00-{:02d}:15".format(hours, hours)

        result.append([key, text])
        print(key)

    return result


def reader(url, timeout: int):
    """ Reads data from a file through HTTP"""
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()


def multi_threaded_reader(urls, num_threads: int) -> List[str]:
    """
        Read multiple files through HTTP
    """
    result: List[str] = []
    for url in urls:
        data = reader(url, constants.URL_TIMEOUT)
        data: str = data.decode('utf-8')
        result.extend(data.split("\n"))
    result: List[str] = sorted(result, key=lambda elem:elem[1])
    return result


