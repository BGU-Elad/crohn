
class Session:
    def __init__(self, morning=None, evening=None):
        self.start = morning
        self.end = evening
        self.date = morning.date() if morning is not None else evening.date()

    def count(self):
        count = 0
        if self.start is not None:
            count += 1
        if self.end is not None:
            count += 1
        return count
