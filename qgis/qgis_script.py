from datetime import datetime
from qgis import processing

grid_spacing = ## GRID SIZE IN METERS eg. 5
work_directory = ## ADD WORK DIRECTORY
grid_output = work_directory + 'grid.gpkg'
work_area_extend = ## ADD EXTEND eg.  '600117.1269789624493569,610117.1269789624493569,5035998.5228524049744010,5045998.5228524049744010 [EPSG:32618]'
working_crs = 'EPSG:32618'

input_trajectory_files = ## ADD GPS TRAJECTORY FILE LIST eg. ["mtl_2016_test_10k_5m.csv", "mtl_2016_test_10k_10m.csv"]

print("Starting create grid:")
print("==============================================")
starttime = datetime.now()
print('Start time : ' + str(starttime))


processing.run('qgis:creategrid',
	{ 'CRS' : QgsCoordinateReferenceSystem(working_crs), 
	'EXTENT' : work_area_extend, 
	'HOVERLAY' : 0, 
	'HSPACING' : grid_spacing, 
	'OUTPUT' : grid_output, 
	'TYPE' : 2, 
	'VOVERLAY' : 0, 
    'VSPACING' : grid_spacing }
    )

endtime = datetime.now()
print('End time : ' + str(endtime))
print('Process took : ' + str(endtime - starttime))
print("Create grid ended.")
print("==============================================")

for file in input_trajectory_files:
	print("Starting reprojection of " + file)
	print("==============================================")
	starttime = datetime.now()
	print('Start time : ' + str(starttime))

	input_path = "file://"+ work_directory + file + '?type=csv&detectTypes=yes&xField=longitude&yField=latitude&crs=EPSG:4326&spatialIndex=no&subsetIndex=no&watchFile=no'
	input_file = QgsVectorLayer(input_path,"input_trajectory", "delimitedtext")
	output_path = work_directory + 'temp_trajectory_projected.gpkg'

	processing.run("qgis:reprojectlayer", 
		{ 'INPUT' : input_file, 
		'OUTPUT' : output_path, 
		'TARGET_CRS' : QgsCoordinateReferenceSystem('EPSG:32618') }
		)

	endtime = datetime.now()
	print('End time : ' + str(endtime))
	print('Process took : ' + str(endtime - starttime))
	print("Reprojection of " + file + " ended.")
	print("==============================================")

	print("Starting spatial join of " + file)
	print("==============================================")
	starttime = datetime.now()
	print('Start time : ' + str(starttime))

	input_path = work_directory + 'temp_trajectory_projected.gpkg'
	output_path = work_directory + 'temp_sptl_grid.gpkg'

	processing.run("qgis:joinbylocationsummary", 
		{ 'DISCARD_NONMATCHING' : False, 
		'INPUT' : grid_output, 
		'JOIN' : input_path, 
		'JOIN_FIELDS' : ['speed'], 
		'OUTPUT' : output_path, 
		'PREDICATE' : [0], 
		'SUMMARIES' : [0,3,6]}
		)

	endtime = datetime.now()
	print('End time : ' + str(endtime))
	print('Process took : ' + str(endtime - starttime))
	print("Spatial join of " + file + " ended.")
	print("==============================================")

	print("Starting rasterization of " + file)
	print("==============================================")
	starttime = datetime.now()
	print('Start time : ' + str(starttime))

	input_path = work_directory + 'temp_sptl_grid.gpkg'
	output_path = work_directory + '' + file + 'count.tif'

	processing.run("gdal:rasterize", 
		{ 'BURN' : 0, 
		'DATA_TYPE' : 5, 
		'EXTENT' : work_area_extend, 
		'EXTRA' : '', 
		'FIELD' : 'speed_count', 
		'HEIGHT' : grid_spacing, 
		'INIT' : None, 
		'INPUT' : input_path, 
		'INVERT' : False, 
		'NODATA' : 0, 
		'OPTIONS' : '', 
		'OUTPUT' : output_path, 
		'UNITS' : 1, 
		'WIDTH' : grid_spacing }
		)

	output_path = work_directory + '' + file + 'mean.tif'
	processing.run("gdal:rasterize", 
		{ 'BURN' : 0, 
		'DATA_TYPE' : 5, 
		'EXTENT' : work_area_extend, 
		'EXTRA' : '', 
		'FIELD' : 'speed_mean', 
		'HEIGHT' : grid_spacing, 
		'INIT' : None, 
		'INPUT' : input_path, 
		'INVERT' : False, 
		'NODATA' : 0, 
		'OPTIONS' : '', 
		'OUTPUT' : output_path, 
		'UNITS' : 1, 
		'WIDTH' : grid_spacing }
		)

	output_path = work_directory + '' + file + 'max.tif'
	processing.run("gdal:rasterize", 
		{ 'BURN' : 0, 
		'DATA_TYPE' : 5, 
		'EXTENT' : work_area_extend, 
		'EXTRA' : '', 
		'FIELD' : 'speed_max', 
		'HEIGHT' : grid_spacing, 
		'INIT' : None, 
		'INPUT' : input_path, 
		'INVERT' : False, 
		'NODATA' : 0, 
		'OPTIONS' : '', 
		'OUTPUT' : output_path, 
		'UNITS' : 1, 
		'WIDTH' : grid_spacing }
		)

	endtime = datetime.now()
	print('End time : ' + str(endtime))
	print('Process took : ' + str(endtime - starttime))
	print("Rasterization of " + file + " ended.")
	print("==============================================")
