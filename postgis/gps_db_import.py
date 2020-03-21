import psycopg2
from datetime import datetime

working_directory = '/home/nagellette/Desktop/mtl_test/'
host = 'localhost'
database = 'gisdb'
user = 'gisdb'
password = '123456'

files = ['_mtl_2016_test_10k_5m.csv', 
'_mtl_2016_test_15k_5m.csv', 
'_mtl_2016_test_20k_5m.csv', 
'_mtl_2016_test_25k_18m.csv', 
'_mtl_2016_test_30k_15m.csv', 
'_mtl_2016_test_15k_10m.csv', 
'_mtl_2016_test_20k_10m.csv', 
'_mtl_2016_test_25k_10m.csv', 
'_mtl_2016_test_25k_5m.csv', 
'_mtl_2016_test_30k_20m.csv', 
'_mtl_2016_test_15k_11m.csv', 
'_mtl_2016_test_20k_15m.csv', 
'_mtl_2016_test_25k_15m.csv', 
'_mtl_2016_test_30k_10m.csv', 
'_mtl_2016_test_30k_5m.csv']

conn = psycopg2.connect(host = host, database = database, user = user, password = password)



for file_name in files:
	print("========================================")
	print("Importing: " + file_name)

	start_time = datetime.now()
	print("Start time " + str(start_time))

	cur = conn.cursor()

	create_statement = 'CREATE TABLE ' + file_name[:-4] + ' (geom geometry(Point,4326), c0 integer, id_coord integer, latitude double precision, longitude double precision, speed double precision, "timestamp" character varying, id_trip integer, bearing double precision)'

	cur.execute(create_statement)

	cur.close()
	conn.commit()

	f = open(working_directory + file_name)

	cur = conn.cursor()
	cur.copy_from(f, file_name[:-4], columns = ('c0','id_coord','latitude','longitude','speed','timestamp','id_trip','bearing'), sep = ',')

	cur.close()
	conn.commit()

	cur = conn.cursor()

	geo_statement = "UPDATE " + file_name[:-4] + " SET geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326);"

	cur.execute(geo_statement)

	cur.close()
	conn.commit()

	end_time = datetime.now()

	print("End time " + str(end_time))
	print("Process took : " + str(end_time - start_time))

print("========================================")
conn.close()