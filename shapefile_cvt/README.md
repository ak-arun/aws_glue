https://sedona.apache.org/latest/tutorial/jupyter-notebook/
https://sedona.apache.org/latest/setup/install-python/
--extra-jars geotools-wrapper-1.7.1-28.5.jar, sedona-spark-shaded-3.4_2.12-1.7.1.jar
--additional-python-modules apache-sedona,geopandas
--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions
