import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext



def sample_func():
    import fiona.crs
    import geopandas as gpd
    
    # read_json_file = sqlContext.read.json(in_file)
    in_file = 'hdfs:///tmp/bdm/neighborhoods.geojson'
    zones = gpd.read_file(in_file).to_crs(fiona.crs.from_epsg(2263))
    print(zones)
    # print(read_json_file.printSchema())
    # zones = gpd.read_file(shapefile)
    # print(zones.take(10))


if __name__=='__main__':
    import fiona.crs
    import geopandas as gpd
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession(sc)

    input_file = sys.argv[1]

    taxi = sc.textFile(input_file)
    # print(list(enumerate(taxi.first().split(','))))

    # neighborhoods = sqlContext.read.json('hdfs:///tmp/bdm/neighborhoods.geojson')
    # print(neighborhoods.printSchema())
    # boroughs = sqlContext.read.json('hdfs:///tmp/bdm/boroughs.geojson')


    # neighborhoods = sc.textFile('hdfs:///tmp/bdm/neighborhoods.geojson')
    
    sample_func()
    # sample_func('hdfs:///tmp/bdm/boroughs.geojson')
    # sample_func(boroughs)


