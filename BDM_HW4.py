import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext



def sample_func(in_file):
    import fiona.crs
    import geopandas as gpd
    
    read_json_file = sqlContext.read.json(in_file)
    print(read_json_file.printSchema())
    # zones = gpd.read_file(shapefile)
    # print(zones.take(10))


if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    input_file = sys.argv[1]

    taxi = sc.textFile(input_file)
    print(list(enumerate(taxi.first().split(','))))

    # neighborhoods = sqlContext.read.json('hdfs:///tmp/bdm/neighborhoods.geojson')
    # print(neighborhoods.printSchema())
    # boroughs = sqlContext.read.json('hdfs:///tmp/bdm/boroughs.geojson')


    # neighborhoods = sc.textFile('hdfs:///tmp/bdm/neighborhoods.geojson')
    
    sample_func('hdfs:///tmp/bdm/neighborhoods.geojson')
    sample_func('hdfs:///tmp/bdm/boroughs.geojson')
    # sample_func(boroughs)


