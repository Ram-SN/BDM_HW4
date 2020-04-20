import sys
import pyspark
from pyspark import SparkContext

def sample_func(shapefile):
    import fiona.crs
    import geopandas as gpd

    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    print(zones.take(10))



if __name__=='__main__':
    sc = SparkContext()
    input_file = sys.argv[1]
    taxi = sc.textFile(input_file)
    print(taxi.take(10))
    sample_func('hdfs:///tmp/bdm/neighborhoods.geojson')
    sample_func('hdfs:///tmp/bdm/boroughs.geojson')


