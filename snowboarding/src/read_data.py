import pandas as pd
import json
import os
import xml.etree.ElementTree as ET
from dateutil import parser
from zipfile import ZipFile
from io import BytesIO

############### Setup ###############


# Base location of the data
base = 'C:/Users/stsztu/OneDrive - SAS/Documents/git/viz/keystone/data/'

# Individual data folders
gps_loc      = base + 'gps'
hr_loc       = base + 'biometrics/hr/'
hr_var_loc   = base + 'biometrics/hr_variability/'
spo2_loc     = base + 'biometrics/spo2/'
spo2_var_loc = base + 'biometrics/spo2_variability/'

############### End Setup ###############
    
# Reads JSON heartrate data from a location and returns a dataframe
def read_bio_json(loc):
    df_list  = []
    filelist = [f for f in os.listdir(loc) if f.endswith('.json')]
    
    for filename in filelist:
        f = os.path.join(loc, filename)
        with open(f, 'r') as file:
            data = json.load(file)
        
        df = pd.json_normalize(data, sep='_')
        df.columns = df.columns.str.lower().str.replace('value_', '')
        df['datetime'] = ( pd.to_datetime(df['datetime'], format='%m/%d/%y %H:%M:%S', utc=True)
                           .dt.tz_convert('US/Mountain')
                           .dt.tz_localize(None)
                         )
        df_list.append(df)
        
    # Final heartrate dataframe 
    return pd.concat(df_list, ignore_index=True)
    
# Reads biometric CSV files from a location and returns a dataframe
def read_bio_csv(loc):    
    df_list  = []
    filelist = [f for f in os.listdir(loc) if f.endswith('.csv')]
    
    for filename in filelist:
        f = os.path.join(loc, filename)
        with open(f, 'r') as file:
            df = pd.read_csv(f, parse_dates=['timestamp'])
        
        df_list.append(df)
        
    # Final heartrate dataframe 
    return pd.concat(df_list, ignore_index=True)



''' Read GPS data in GPX format without needing to import a separate GPX 
    package. GPX data looks like this:
        
    <?xml version="1.0" encoding="UTF-8"?>
    <gpx xmlns="http://www.topografix.com/GPX/1/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:gte="http://www.gpstrackeditor.com/xmlschemas/General/1" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd" version="1.1" creator="Slopes for Android - http://getslopes.com">
      <trk>
        <name>Jan 25, 2024 - Keystone Resort</name>
        <trkseg>
          <trkpt lat="39.605675" lon="-105.941414">
            <ele>2856.891977</ele>
            <time>2024-01-25T09:13:52.453-07:00</time>
            <hdop>19</hdop>
            <vdop>4</vdop>
            <extensions>
              <gte:gps speed="1.317580" azimuth="212.300003"/>
            </extensions>
          </trkpt>
       </trkseg>
      </trk>
    </gpx>
    
    There are two namespaces we need to use:
        1. The gpx namespace: http://www.topografix.com/GPX/1/1
        2. The gte namespace http://www.gpstrackeditor.com/xmlschemas/General/1
        
    The gte namespace is used to extract gps and azimuth data from the 
    <extensions> tag
'''

def read_gpx(loc):
    gpx_namespace = '{http://www.topografix.com/GPX/1/1}'
    gte_namespace = '{http://www.gpstrackeditor.com/xmlschemas/General/1}'
    
    data     = []
    filelist = [f for f in os.listdir(loc) if f.endswith('.gpx')]
    
    for filename in filelist:
        f    = os.path.join(gps_loc, filename)
        root = ET.parse(f)
        
        for trkpt in root.findall(f'.//{gpx_namespace}trkpt'):
            row = {
                "datetime": parser.parse(trkpt.find(f'{gpx_namespace}time').text, ignoretz=True),
                "lat": float(trkpt.get("lat")),
                "lon": float(trkpt.get("lon")),
                "elevation": float(trkpt.find(f'{gpx_namespace}ele').text),
                "speed": float(trkpt.find(f'.//{gpx_namespace}extensions/{gte_namespace}gps').get("speed")),
                "azimuth": float(trkpt.find(f'.//{gpx_namespace}extensions/{gte_namespace}gps').get("azimuth"))
            }
        
            data.append(row)

    # Final GPS dataframe
    return pd.DataFrame(data)


# Read in GPS metadata. This will help us more easily define runs and lifts
# and also give us some additonal information if we want to use it
def read_gps_metadata(loc):    
    df_list  = []
    filelist = [f for f in os.listdir(loc) if f.endswith('.slopes')]
    
    # .slopes files are just zip files with some CSVs and XML metadata.
    # We just want to read Metadata.xml
    for filename in filelist:
        zip_file_path = os.path.join(loc, filename)
        
        with ZipFile(zip_file_path, 'r') as zip_file:
            with zip_file.open('Metadata.xml') as xml_file:
                df = pd.read_xml(xml_file, parser='etree', xpath='.//Action')
            
        # Convert start/end to datetimes without the timezone
        df[['start', 'end']] = df[['start', 'end']].applymap(lambda x: parser.parse(x, ignoretz=True))
        df_list.append(df)
        
    # Final GPS metadata dataframe 
    return pd.concat(df_list, ignore_index=True)

######## Read in all the data ########
df_hr       = read_bio_json(hr_loc)
df_var_hr   = read_bio_csv(hr_var_loc)
df_spo2     = read_bio_csv(spo2_loc)
df_spo2_var = read_bio_csv(spo2_var_loc)
df_gps      = read_gpx(gps_loc)
df_gps_meta = read_gps_metadata(gps_loc)


df_gps.to_csv(r'C:\Users\stsztu\OneDrive - SAS\Documents\My SAS Files\keystone\GPS.csv')
