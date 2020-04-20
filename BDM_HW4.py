import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import geopandas as gpd



# def sample_func(shapefile):
#     import fiona.crs
#     import geopandas as gpd

#     zones = gpd.GeoDataFrame(shapefile)
#     print(zones.take(10))



if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)
    input_file = sys.argv[1]
    taxi = sc.textFile(input_file)
    print(taxi.take(10))
    neighborhoods = 'hdfs:///tmp/bdm/neighborhoods.geojson'
    zones = gpd.read_file(neighborhoods).to_crs(fiona.crs.from_epsg(2263))
    print(zones.head())
    #print(neighborhoods.printSchema())
    # neighborhoods = sc.textFile('hdfs:///tmp/bdm/neighborhoods.geojson')
    # boroughs = sc.textFile('hdfs:///tmp/bdm/boroughs.geojson')
    # sample_func(neighborhoods)
    # # sample_func(boroughs)


