import re

class comma_regex():
    COMMA_REGEX = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

class housePriceTuple():
    # Note their is currently as issue where this class needs to be in a seperate file to work with executors
    def __init__(self, count: int, total: float):
        self.count = count
        self.total = total