'''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''


import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    conf = SparkConf().setAppName('sum_of_numbers').setMaster('local[*]')
    sc = SparkContext(conf = conf)

    # Read in RDD
    lines = sc.textFile('../data/prime_nums.text')
    # Split the tab seperated lines
    numbers = lines.flatMap(lambda line: line.split("\t"))
    # Filter out empty strings which can occur while splitting above
    validNumbers = numbers.filter(lambda number: number)
    # convert strings to ints
    intNumbers = validNumbers.map(lambda number: int(number))
    
    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))

