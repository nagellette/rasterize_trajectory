from osgeo import gdal, osr
import numpy as np
from datetime import datetime
from pyproj import Proj, transform
import pandas as pd
import swifter
from tqdm import tqdm

tqdm.pandas("Pandas progress bar.")

output_epsg = 32618
input_epsg = 4326

path = "./"
image_path = "test_areas/"

input_list = [
    ["mtl_2016_test_10k_5m.csv", "small_test_area10k.tif"],
    ["mtl_2016_test_15k_5m.csv", "small_test_area15k.tif"],
    ["mtl_2016_test_15k_10m.csv", "small_test_area15k.tif"],
    ["mtl_2016_test_15k_11m.csv", "small_test_area15k.tif"],
    ["mtl_2016_test_20k_5m.csv", "small_test_area20k.tif"],
    ["mtl_2016_test_20k_10m.csv", "small_test_area20k.tif"],
    ["mtl_2016_test_20k_15m.csv", "small_test_area20k.tif"],
    ["mtl_2016_test_25k_5m.csv", "small_test_area25k.tif"],
    ["mtl_2016_test_25k_10m.csv", "small_test_area25k.tif"],
    ["mtl_2016_test_25k_15m.csv", "small_test_area25k.tif"],
    ["mtl_2016_test_25k_18m.csv", "small_test_area25k.tif"],
    ["mtl_2016_test_30k_5m.csv", "small_test_area30k.tif"],
    ["mtl_2016_test_30k_10m.csv", "small_test_area30k.tif"],
    ["mtl_2016_test_30k_15m.csv", "small_test_area30k.tif"],
    ["mtl_2016_test_30k_20m.csv", "small_test_area30k.tif"]
]


## extend function
def GetExtent(gt, cols, rows):
    ''' Return list of corner coordinates from a geotransform

        @type gt:   C{tuple/list}
        @param gt: geotransform
        @type cols:   C{int}
        @param cols: number of columns in the dataset
        @type rows:   C{int}
        @param rows: number of rows in the dataset
        @rtype:    C{[float,...,float]}
        @return:   coordinates of each corner

    Source: https://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings
    '''
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([x, y])
        yarr.reverse()
    return ext


## casting transform values to python array to enable pandas apply
def convert_array_epsg(coordinates, input_epsg, output_epsg):
    fx, fy = transform(input_epsg, output_epsg, coordinates[:, 1], coordinates[:, 0])
    # Re-create (n,2) coordinates
    return np.dstack([fx, fy])[0]


def return_row_column_lon_lat(values):
    local_coord = values["lat_new"] - values["corner_y"]
    residual = local_coord % values["step_y"]
    count_lat = (local_coord - residual) / values["step_y"]

    local_coord = values["lon_new"] - values["corner_x"]
    residual = local_coord % values["step_x"]
    count_lon = (local_coord - residual) / values["step_x"]

    return str(abs(count_lat.astype(int))) + "_" + str(abs(count_lon.astype(int)))


for files in input_list:
    # input and working path
    input_file = files[0]

    print("\n")
    print("Processing starting for " + input_file)
    print("===================================================")

    ## path to label raster
    raster_path_label = path + image_path + files[1]

    ## read template raster file
    raster_label = gdal.Open(raster_path_label)
    raster_label_band = raster_label.GetRasterBand(1)

    ## get pixel counts by axis
    raster_x_count = raster_label.RasterXSize
    raster_y_count = raster_label.RasterYSize

    ## get transformation parameters
    raster_label_geo_transform = raster_label.GetGeoTransform()

    ## get template corner coordinates
    raster_x_corner = raster_label_geo_transform[0]
    raster_y_corner = raster_label_geo_transform[3]

    ## get template pixel size by axis
    raster_x_step = raster_label_geo_transform[1]
    raster_y_step = raster_label_geo_transform[5]

    ## create empty arrays for raster bands
    trajectory_count = np.zeros((raster_y_count, raster_x_count), dtype=np.float32)
    speed_avg = np.zeros((raster_y_count, raster_x_count), dtype=np.float32)
    speed_max = np.zeros((raster_y_count, raster_x_count), dtype=np.float32)

    ## create empty raster with 4 bands to produce final raster
    output_raster = gdal.GetDriverByName('GTiff').Create(path + input_file + '.tif', raster_x_count,
                                                         raster_y_count, 3,
                                                         gdal.GDT_Float32)
    output_raster.SetGeoTransform(raster_label_geo_transform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(output_epsg)
    output_raster.SetProjection(srs.ExportToWkt())  # Exports the coordinate system to the file

    ## counter for progress
    counter = 0.0
    total_count = float(raster_y_count) * float(raster_x_count)

    ## time function for time progress
    start = datetime.now()
    print("Process started at ", start)

    trajectory = pd.read_csv(path + input_file)
    trajectory = trajectory.drop(['id_coord', 'timestamp', 'id_trip'], axis=1)

    ## calculate raster lower corner coordinates
    raster_x_corner_ = raster_x_corner + (raster_x_count * raster_x_step)
    raster_y_corner_ = raster_y_corner + (raster_y_count * raster_y_step)

    trajectory["input_sys"] = input_epsg
    trajectory["output_sys"] = output_epsg

    lon_df = pd.DataFrame(trajectory['longitude'])
    lat_df = pd.DataFrame(trajectory['latitude'])
    trajectory_temp = pd.concat([lon_df, lat_df], axis=1)
    trajectory_np = trajectory_temp.to_numpy()

    start_reproject = datetime.now()
    ## calculating raster coordinates of input points
    trajectory_temp = convert_array_epsg(trajectory_np, input_epsg, output_epsg)

    trajectory_temp = pd.DataFrame(trajectory_temp, columns=["lon_new", "lat_new"])

    trajectory = pd.concat([trajectory, trajectory_temp], axis=1)

    end_reproject = datetime.now()
    print("Reproject took " + str(end_reproject - start_reproject))

    ## intermediate values
    trajectory["corner_x"] = raster_x_corner
    trajectory["corner_y"] = raster_y_corner

    trajectory["step_x"] = raster_x_step
    trajectory["step_y"] = raster_y_step

    start_join = datetime.now()

    ## calculate row and column number of each point in raster grid
    # trajectory["count_lon_lat"] = trajectory.swifter.progress_bar(True).apply(lambda x: return_row_column_lon_lat(x),
    #                                                                           axis=1)

    trajectory["count_lon_lat"] = trajectory.progress_apply(lambda x: return_row_column_lon_lat(x),
                                                                             axis=1)

    ## aggregate points to grids for feature generation
    trajectory_agg = trajectory.groupby(["count_lon_lat"]).agg(
        point_count=pd.NamedAgg(column="speed", aggfunc="count"),
        speed_avg=pd.NamedAgg(column="speed", aggfunc="mean"),
        speed_max=pd.NamedAgg(column="speed", aggfunc="max")).reset_index(["count_lon_lat"])

    ## write aggregated values to raster input arrays
    for index, row in tqdm(trajectory_agg.iterrows(), total=trajectory_agg.shape[0]):
        dash_loc = row["count_lon_lat"].find("_")
        i = int(row["count_lon_lat"][0:dash_loc])
        end_loc = len(row["count_lon_lat"])
        j = int(row["count_lon_lat"][dash_loc + 1:end_loc])
        if i < raster_y_count and j < raster_x_count:
            trajectory_count[i, j] = int(row["point_count"])
            speed_avg[i, j] = row["speed_avg"]
            speed_max[i, j] = row["speed_max"]

    end_join = datetime.now()

    print("Spatial join took " + str(end_join - start_join))

    start_save_raster = datetime.now()

    ## fill the output bands
    output_raster.GetRasterBand(1).WriteArray(trajectory_count)
    output_raster.GetRasterBand(2).WriteArray(speed_avg)
    output_raster.GetRasterBand(3).WriteArray(speed_max)

    ## close raster files
    output_raster = None
    raster_label = None

    end_save_raster = datetime.now()

    print("Save raster took " + str(end_save_raster - start_save_raster))

    ## write processing time
    end = datetime.now()
    print("Process ended at ", end)
    print("Took: ", end - start)
    print("Processing ended for " + input_file)
    print("===================================================")
