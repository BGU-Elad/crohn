from src.utils.constants import RELATIVE_DELTA_EPSILON, MINUS_TIME
import datetime


def calc_relative_delta(metric1, metric2):
    return (metric1 - metric2) / (metric1 + RELATIVE_DELTA_EPSILON)


def take_two_from_each_cycle(lists):
    result = []
    while any(lists):
        for lst in lists:
            if len(lst) >= 2:
                result.extend(lst[:2])
                del lst[:2]
            elif len(lst) == 1:
                result.append(lst[0])
                del lst[0]
    return result


def get_first_or_empty(lst):
    if lst is None or len(lst) == 0:
        return ['']
    return lst[0]


def get_now(minus=MINUS_TIME):
    return datetime.datetime.now() - datetime.timedelta(days=minus)


class ExerciseData:
    def __init__(self, exercise=None, technique=None, change=None, relative_delta=None, count=None, in_out_unit=None):
        self.exercise = exercise
        self.technique = technique
        self.change = change
        self.relative_delta = relative_delta
        self.count = count
        self.in_out_unit = in_out_unit

    def __dict__(self):
        d = {
            "exercise": self.exercise,
            "technique": self.technique,
            "change": self.change,
            "relative_delta": self.relative_delta,
            "count": self.count,
            "in_out_unit": self.in_out_unit
        }
        return {k: v for k, v in d.items() if v is not None}
