import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


# def sample_func(shapefile):
#     import fiona.crs
#     import geopandas as gpd

#     zones = gpd.read_file(shapefile)
#     print(zones.take(10))



if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    input_file = sys.argv[1]
    taxi = sc.textFile(input_file)
    print(taxi.take(10))

    neighborhoods = spark.read.json('hdfs:///tmp/bdm/neighborhoods.geojson')
    neighborhoods = neighborhoods.toDF()
    print(neighborhoods)
    # neighborhoods = sc.textFile('hdfs:///tmp/bdm/neighborhoods.geojson')
    # boroughs = sc.textFile('hdfs:///tmp/bdm/boroughs.geojson')
    # sample_func(neighborhoods)
    # sample_func(boroughs)


