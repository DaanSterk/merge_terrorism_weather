from netCDF4 import Dataset, netcdftime, num2date
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from datetime import datetime

# Terrorism dataset
t_set = pd.read_csv('globalterrorismdb_0617dist.csv', encoding='ISO-8859-1')

data_columns = [

    # ===== Spatio-Temporal Variables =====
    # The names of these variables speak for themselves;
    # where in time and space was the act of terrorism committed?
    'iyear', 'imonth', 'iday', 'latitude', 'longitude',

    # ===== Binary Variables (1 -> yes or 0 -> no) =====
    'extended',  # Did the duration of the incident extend 24 hours?
    'vicinity',  # Did the incident occur in the immediate vicinity of the city? Is 0 for IN city.
    'crit1', 'crit2', 'crit3',  # The incident meets the criterion (1, 2, 3), described in the introduction.
    'doubtterr',  # Is there doubt to wether the attack is an act of terrorism?
    'multiple',  # Is this incident connected to other incident(s)? !! Consistently available since 1997 !!
    'success',  # Has the attack reached its goal? Depends on type of attack.
    'suicide',  # Did the perpetrator intend to escape alive?
    'claimed',  # Was the attack claimed by an organised group?
    'property',  # Is there evidence of property damage from the incident?
    'ishostkid',  # Were there victims taken hostage or kidnapped?

    # ===== Continuous Variables =====
    'nkill',  # Amount of confirmed kills.
    'nwound',  # Amount of confirmed wounded.

    # ===== Categorical variables (textual) =====
    'country_txt',  # Name of country.
    'region_txt',  # Name of region.
    'attacktype1_txt',  # Of what type was the attack? I.e. assassination, bombing or kidnapping.
    'targtype1_txt',  # What target did the attack have? I.e. business, government or police.
    'natlty1_txt',  # Nationality of the target.
    'weaptype1_txt',  # What weapon was used?

    # ===== Descriptive Variables =====
    'target1',  # Description of specific target, if applicable.
    'gname',  # Name of the organized group, if applicable.
    'summary',  # Summary of the attack.

]

t_set = t_set[t_set.iyear >= 2012]
t_set = t_set.loc[:, data_columns] # Only keep described columns.

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
    if row.iyear >= 2012:  # Weather data starts at 2012.
        if row.iyear == 2017:
            if row.imonth > 6:  # Only check the first six months of 2017.
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

t_set.to_csv("terrorist_weather_jan2012_jul2017.csv", index=False)
print("File write complete.")
