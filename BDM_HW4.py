import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext



def sample_func():
    import fiona.crs
    import geopandas as gpd
    
    #in_file = 'neighborhoods.geojson'
    zones = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))
    print(zones.head())
    in_file = 'boroughs.geojson'
    zones = gpd.read_file(in_file).to_crs(fiona.crs.from_epsg(2263))
    print(zones.head())


if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    input_file = sys.argv[1]

    taxi = sc.textFile(input_file)
    
    sample_func()
    

