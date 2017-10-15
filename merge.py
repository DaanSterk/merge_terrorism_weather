from netCDF4 import Dataset, netcdftime, num2date
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from datetime import datetime

# Terrorism dataset
t_set = pd.read_csv('globalterrorismdb_0617dist.csv', encoding='ISO-8859-1')

# Weather dataset
w_set = Dataset('w_db_part.nc')
print("Data loaded")

w_lats = w_set.variables['latitude'][:]  # Used to index
w_lons = w_set.variables['longitude'][:]  # Used to index
w_time = w_set.variables['time'][:]  # Used to index
w_t2m = w_set.variables['t2m'][:]  # 2 meter temperature.
w_tcc = w_set.variables['tcc'][:]  # Total cloud cover
w_vidgf = w_set.variables['p85.162'][:]  # Vertical integral of divergence of geopotential flux
w_sp = w_set.variables['sp'][:]  # Surface pressure
w_v10 = w_set.variables['v10'][:]  # 10 metre V wind component

w_set.close()  # Close weather set.


# Returns index of nearest lat value in weather coords.
# - Param target_val: the latitude value from terrorist db.
def t_lat_to_w_lat_index(target_val):
    return np.abs(w_lats - target_val).argmin()


# Returns index of nearest lon value in weather coords.
# - Param target_val: the longitude value from terrorist db.
def t_lon_to_w_lon_index(target_val):
    base_dif = 180  # Lons in weather data go from 0 - 360
    # whereas in the terrorist db they go from -180 to 180
    return np.abs(w_lons - (target_val + base_dif)).argmin()


# Returns amount of steps since epoch time.
def days_from_epoch(year, month, day):
    epoch_datetime = datetime(2012, 1, 1)  # First record in weather data.
    this_datetime = datetime(year, month, day)
    return (this_datetime - epoch_datetime).days


# Connect a weather db column to 
def connect(row, w_col):
    if row.iyear > 2012:  # Weather data starts at 2012.
        if row.iyear == 2017:
            if row.imonth > 6:  # Only check the first 7 months of 2017.
                return None
        epoch_time = days_from_epoch(row.iyear, row.imonth, row.iday)
        lat_index = t_lat_to_w_lat_index(row.latitude)
        lon_index = t_lon_to_w_lon_index(row.longitude)
        return w_col[epoch_time, lat_index, lon_index]
    return None

t_set['t2m'] = t_set.apply(lambda row: connect(row, w_t2m), axis=1)
print("t2m merged")
t_set['tcc'] = t_set.apply(lambda row: connect(row, w_tcc), axis=1)
print("tcc merged")
t_set['vidgf'] = t_set.apply(lambda row: connect(row, w_vidgf), axis=1)
print("vidgf merged")
t_set['sp'] = t_set.apply(lambda row: connect(row, w_sp), axis=1)
print("sp merged")
t_set['v10'] = t_set.apply(lambda row: connect(row, w_v10), axis=1)
print("v10 merged")

t_set.to_csv("terrorist_weather_jan2012_jul2017.csv")
print("File write complete.")
