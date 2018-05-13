'''
Create a Spark program to read the house data from in/RealEstate.csv,
output the average price for houses with different number of bedrooms.

The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
around it. 

The dataset contains the following fields:
1. MLS: Multiple listing service number for the house (unique ID).
2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
some out of area locations as well.
3. Price: the most recent listing price of the house (in dollars).
4. Bedrooms: number of bedrooms.
5. Bathrooms: number of bathrooms.
6. Size: size of the house in square feet.
7. Price/SQ.ft: price of the house per square foot.
8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

Each field is comma separated.

Sample output:

    (3, 325000)
    (1, 266356)
    (2, 325000)
    ...

3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
'''

import utils
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName('avg_house_price').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    lines = sc.textFile('../data/RealEstate.csv')
    cleaned_lines = lines.filter(lambda line: "Bedrooms" not in line)
    
    house_price_pairs = cleaned_lines.map(lambda line: \
     (line.split(',')[3], utils.housePriceTuple(1,float(line.split(',')[2]))))

    house_price_total = house_price_pairs.reduceByKey(lambda x,y: utils.housePriceTuple(x.count + y.count, x.total + y.total))

    house_price_avg = house_price_total.mapValues(lambda housePriceTuple: housePriceTuple.total/housePriceTuple.count)

    print("House total price:")
    for bedroom in house_price_avg.collect():
        print("{}".format(bedroom))

