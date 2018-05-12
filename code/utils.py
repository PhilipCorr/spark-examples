import re

class comma_regex():

    COMMA_REGEX = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
