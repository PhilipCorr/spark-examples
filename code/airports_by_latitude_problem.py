'''
Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

Each row of the input file contains the following columns:
Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

Sample output:
"St Anthony", 51.391944
"Tofino", 49.082222
...
'''

import sys
# import the current working directory
sys.path.insert(0, '.')

import utils
from pyspark import SparkContext, SparkConf

def splitComma(line: str):
    splits = utils.comma_regex.COMMA_REGEX.split(line)
    # name of airport and latitude
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("Airport_Latitude").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("../data/airports.text")
    # return airports with latitude over 40
    airports_with_lat = airports.filter(lambda line: float(utils.comma_regex.COMMA_REGEX.split(line)[6]) > 40)

    airports_name_and_lat = airports_with_lat.map(splitComma)
    airports_name_and_lat.saveAsTextFile("../results/airports_lat_over_40.text")

