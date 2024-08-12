import logging
import yaml
import pandas as pd
from pandas import read_csv
import dask
import dask.dataframe
import time
import re


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, pattern= 'in',by= ' '):
    # Replaces a pattern by whitespace
    string = re.sub(pattern, by, string) 
    return string




def read_iteratively_and_perform(operations_function, table_config):
    """
    This function is the main one of the pipeline. It reads a satisfyingly large 
    portion of the dataset in chunks and returns a preview of the data along with 
    the mean values of three columns after cleaning them.
    
    Inputs: 
        operations_function: The function to iteratively apply to the chunks.
        table_config: Configuration data.
    Outputs:
        preview, means
    """




    tot_rows_to_read = 100000
    
    chunk_size = 1000
    
    df = read_csv('cars_data.csv', header=0, names=
                 ['id', 'back_legroom (in)', 'bed', 'bed_height', 'bed_length', 'car_type',
          'cabin', 'city', 'city_fuel_economy', 'combine_fuel_economy',
          'daysonmarket', 'dealer_zip', 'description', 'engine_cylinders',
          'engine_displacement', 'engine_type', 'exterior_color', 'fleet',
          'frame_damaged', 'franchise_dealer', 'franchise_make', 'front_legroom',
          'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height',
          'highway_fuel_economy', 'horsepower', 'interior_color', 'isCab',
          'is_certified', 'is_cpo', 'is_new', 'is_oemcpo', 'latitude', 'length',
          'listed_date', 'listing_color', 'listing_id', 'longitude',
          'main_picture_url', 'major_options', 'make_name', 'maximum_seating',
          'mileage', 'model_name', 'owner_count', 'power', 'price', 'salvage',
          'savings_amount', 'seller_rating', 'sp_id', 'sp_name', 'theft_title',
          'torque', 'transmission', 'transmission_display', 'trimId', 'trim_name',
          'vehicle_damage_category', 'wheel_system', 'wheel_system_display',
          'wheelbase (in)', 'width (in)', 'year'], chunksize = chunk_size)
   
    preview = read_csv('cars_data.csv', header=0, names=
                 ['id', 'back_legroom (in)', 'bed', 'bed_height', 'bed_length', 'car_type',
          'cabin', 'city', 'city_fuel_economy', 'combine_fuel_economy',
          'daysonmarket', 'dealer_zip', 'description', 'engine_cylinders',
          'engine_displacement', 'engine_type', 'exterior_color', 'fleet',
          'frame_damaged', 'franchise_dealer', 'franchise_make', 'front_legroom',
          'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height',
          'highway_fuel_economy', 'horsepower', 'interior_color', 'isCab',
          'is_certified', 'is_cpo', 'is_new', 'is_oemcpo', 'latitude', 'length',
          'listed_date', 'listing_color', 'listing_id', 'longitude',
          'main_picture_url', 'major_options', 'make_name', 'maximum_seating',
          'mileage', 'model_name', 'owner_count', 'power', 'price', 'salvage',
          'savings_amount', 'seller_rating', 'sp_id', 'sp_name', 'theft_title',
          'torque', 'transmission', 'transmission_display', 'trimId', 'trim_name',
          'vehicle_damage_category', 'wheel_system', 'wheel_system_display',
          'wheelbase (in)', 'width (in)', 'year'], nrows= 10)
   
    c = preview.columns.str.lower()
    c = c.map(lambda x: replacer(x,'_'))
    preview.columns = c

    #tests if the columns we are interested in are there.
    for column in table_config['columns']:
        if not ((column in list(c)) and len(c)==66):
            raise Exception('Columns wanted not in given data or missing.')
    
    av_width = 0
    av_back = 0
    av_wheelbase = 0
    tot_len =0
    #we look for the average wheelbase, width and back_legroom.

    start= time.time()
    for chunk in df:
        current_chunk = chunk[['width (in)', 'wheelbase (in)', 'back_legroom (in)']].fillna('none')
        current_chunk = current_chunk.map(lambda x: operations_function(x))
        current_chunk = current_chunk.map(lambda x: operations_function(x, '--', 'none'))
        current_chunk = current_chunk.map(lambda x: operations_function(x,'none','0'))
        current_chunk = current_chunk.astype(float)
        
        means= current_chunk.mean()
        
        av_width+= means['width (in)']
        av_back+= means['back_legroom (in)']
        av_wheelbase += means['wheelbase (in)']

        tot_len += chunk_size
        print('iterating')

        if tot_len== tot_rows_to_read:
            break
    
    end = time.time()
    time_elapsed = end-start
    av_width/= tot_len/chunk_size
    av_back/=tot_len/chunk_size
    av_wheelbase/=tot_len/chunk_size    
        
    print('A preview of the dataset was returned along with means.')
    return av_back,av_wheelbase, av_width, preview, time_elapsed


