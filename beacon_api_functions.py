#Author: Cole Howell
#Purpose: Contains functions used to utilize the Beacon api for getting data from water service meters


import requests
import datetime as dt
import json
import time
import os.path
from arcgis import GIS
import calendar
import configparser


#configure api functions with information from a file
def config():
    config = configparser.ConfigParser()
    config.read('C:\EsriTraining\PythonGP\Scripts\wadc_dev_config.ini')

    #file contains username, password, and api token/parameter information
    gis = config['GIS']
    bcon = config['beacon']

    return gis, bcon


#get the next hour
def next_hour(date):
    return date + dt.timedelta(hours=1)


#authorization parameters
def auth(bcon):
    return bcon['username'], bcon['password']


#required header for posts
def header(bcon):
    return {'Content-Type': bcon['content_type']}


#posts a request to beacon's api to get flow data for the selected routes, returns the uuid and url to request data
def request_service_flow(route_num, s_date, e_date, bcon):
    #required authorization to use api
    a = auth(bcon)
    #required header to make a post to server
    h = header(bcon)

    #parameters needed for data
    params = {
        'Service_Point_Route': f'{route_num}',
        'Start_Date': s_date,
        'End_Date': e_date,
        'Output_Format': 'json',
        'Header_Columns': (
            f'Account_Full_Name,Endpoint_SN,Endpoint_Type,Flow_Time,Flow,Flow_Unit,Location_Address_Line1,'
            f'Current_Leak_Rate,Current_Leak_Start_Date,Backflow_Gallons,Battery_Level'
        ),
        'Resolution': 'Hourly'
    }

    #post a request for data using the data range api, this gets all data between start and end
    resp = requests.post('https://api.beaconama.net/v2/eds/range', params=params, headers=h, auth=a)
    raw = json.loads(resp.content)

    return raw


#posts a request to beacon's server to get monthly flow data for all meters reported on beacon, returns the uuid and url
#to request data
def monthly_meter_audit(s_date, e_date, bcon):
    params = {
        'Start_Date': s_date,
        'End_Date': e_date,
        'Output_Format': 'json',
        'Has_Endpoint': True,
        'Header_Columns': (
            f'Account_Full_Name,Location_Address_Line1,Location_City,Endpoint_SN,'
            f'Flow,Flow_Time,Service_Point_Latitude,Service_Point_Longitude,SA_Start_Date,Read_Method,Account_ID'
        ),
        'Resolution': 'Monthly'
    }
    #post a request for data using the range api, this gets all data between the start and end dates
    resp = requests.post('https://api.beaconama.net/v2/eds/range', params=params, headers=header(bcon), auth=auth(bcon))
    raw = json.loads(resp.content)

    return raw


#requests the status of the processing queue for data retrieval of the passed item
def get_flow_status(raw, bcon):

    #server request to get the status of processing the data request
    resp = requests.get(f'https://api.beaconama.net/v1/eds/status/{raw["edsUUID"]}', auth=auth(bcon))

    #load json into python object
    raw_json = json.loads(resp.content)

    #if the json is just a string and not formatted like a dictionary, return this dictionary
    if isinstance(raw_json, str):
        return {'state': 'queue'}

    return raw_json


#polls the server for the status of the data retrieval, returns the completed status containing the reportURL
def poll_status(raw, bcon):
    #initialize state and status as none
    state = None
    status = None

    #loop until the state is done
    while state != 'done':
        #get the status of the data acquisition
        status = get_flow_status(raw, bcon)

        # the state is just the included state value in the server response
        state = status['state']

        #if the state is either of these pause for 15 seconds then the loop will start again
        if state == 'queue' or state == 'run':
            time.sleep(15.0)

        #if the state throws an exception do this stuff
        elif state == 'exception':
            #define a file path where the data error log is located
            directory = 'C:/Users/chowell/WADC Dropbox/Cole Howell/PC/Documents/Beacon API/'
            file = f'{directory}beacon_data_export_errors.txt'

            #if the file exists append text to it
            if os.path.isfile(file):
                with open(file, 'a') as f:
                    f.write(
                        f'Request {raw["edsUUID"]} experienced a problem at {status["endTime"]} with the following message:'
                        f'\n{status["message"]}\n\n'
                    )
            #otherwise write to a new file
            else:
                with open(file, 'w') as f:
                    f.write(
                        f'Request {raw["edsUUID"]} experienced a problem at {status["endTime"]} with the following message:'
                        f'\n{status["message"]}\n\n'
                    )
            #this will stop the program entirely and will print this to console, will not show in task scheduler though
            raise ValueError('Something went wrong with the data export!')

    return status


#get the data report, accepts a completed status report as an argument
def data_report(status, bcon):
    #this gets the data report from the completed download, that status includes a url to use in get request
    resp = requests.get(f"https://api.beaconama.net/{status['reportUrl']}", auth=auth(bcon))

    #this takes the server response, that is a json, and loads it into a python dictionary
    results = json.loads(resp.content)

    return results


#function for getting all of the data I want for a day
def collect_all(bcon):

    #list of routes in the hwy 96 zone
    routes = ['21', '26', '27', '29']

    #make the routes keys in an empty dictionary
    store = dict.fromkeys(routes, [])

    if calendar.day_name[dt.datetime.today().weekday()] != 'Monday':
        #start and end times passed to the api
        s_time = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=6, minute=0, second=0)
        # s_time = dt.datetime(2023, 12, 28, 6, 0, 0)
    else:
        s_time = (dt.datetime.now() - dt.timedelta(days=3)).replace(hour=6, minute=0, second=0)

    # s_time = (dt.datetime.now() - dt.timedelta(days=4)).replace(hour=7, minute=0, second=0)
    # e_time = (dt.datetime.now() - dt.timedelta(days=3)).replace(hour=7, minute=0, second=0)

    e_time = (dt.datetime.now()).replace(hour=6, minute=0, second=0)
    # e_time = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=6, minute=0, second=0)
    # e_time = dt.datetime(2024, 5, 6, 11, 0, 0)
    # e_time = dt.datetime(2023, 9, 25, 7)
    #loop through the routes in the list
    for r in routes:

        #initialize the post variable
        post = f'{r}'

        #sometimes the post operation can respond with a string instead of a json object, this loops until it is a json
        while isinstance(post, str):
            post = request_service_flow(r, s_time, e_time, bcon)
            # print(post)

        #after successful post, check the status of the data collection, once finished a json link to the results is
        #returned
        status = poll_status(post, bcon)
        data = data_report(status, bcon)

        #this filters the results for only meters with this endpoint type, wireless
        data['results'] = [n for n in data['results'] if n['Endpoint_Type'] == 'J']

        #this puts the results in the appropriate route key from the dictionary
        store[r] = data['results']

        #pause for 10 seconds since the api is rate limited, just makes sure the program doesn't crash
        time.sleep(10.0)

    return store


#function for performing the monthly audit, gets data from 2 months ago that was read last month
def monthly_audit(s_time, e_time, bcon):
    # current_month = dt.datetime.now().replace(day=1, hour=0, minute=0, second=0)
    # last_month = (current_month - dt.timedelta(days=1)).replace(day=1)
    # s_time = current_month.replace(month=current_month.month-2, day=1, hour=0, minute=0, second=0)
    # e_time = (last_month - dt.timedelta(days=1)).replace(hour=23, minute=59, second=59)

    post = monthly_meter_audit(s_time, e_time, bcon)
    status = poll_status(post, bcon)
    data = data_report(status, bcon)

    return [data['results'], s_time, e_time]


#function to store meter data in gis, accepts the data dictionary as an argument
def store_in_gis(store, g):

    #access the badger meter table in gis
    gis = GIS("https://esriapps1.esriwadc.com/portal", g['username'], g['password'])
    meter_layer = gis.content.get('62e76f6d62d543c0ad5c4954e2156efd')
    data_table = meter_layer.tables[0]

    #loop through the data dictionary that was passed in
    for s in store:
        # the dictionary stores data as a list of dictionaries so this loops through the list and accesses those keys
        for i in store[s]:
            # print(i)
            flow = i['Flow']
            leak_rate = i['Current_Leak_Rate']
            backflow = i['Backflow_Gallons']
            leak_date = i['Current_Leak_Start_Date']
            flow_time = i['Flow_Time']

            #check these values for whether they're None and if they are change their value, otherwise convert them
            if flow is None:
                flow = 0.0
            else:
                flow = float(flow)
            if leak_rate is None:
                leak_rate = 0.0
            else:
                leak_rate = float(leak_rate)
            if backflow is None:
                backflow = 0.0
            else:
                backflow = float(backflow)

            #if these are not none they need to be converted to a unix timestamp since that's how gis stores them
            if leak_date is not None:
                leak_date = dt.datetime.strptime(i['Current_Leak_Start_Date'], '%Y-%m-%d %H:%M')
                leak_date = dt.datetime.timestamp(leak_date) * 10**3

            if flow_time is not None:
                f_time = dt.datetime.strptime(i['Flow_Time'], '%Y-%m-%d %H:%M').replace(minute=0)
                flow_time = dt.datetime.timestamp(f_time)*10**3

                #uncomment to only record uncaptured data in event of program failure
                # table_fset = data_table.query(
                #     where=f"flow_time=TIMESTAMP '{f_time}' AND endpoint_sn = '{i['Endpoint_SN']}'")
                # if table_fset.features:
                #     continue


            #portal compatible dictionary used to add new data to the accessed data table
            add = {'attributes':
                       {
                           'account_full_name': i['Account_Full_Name'],
                           'endpoint_sn': i['Endpoint_SN'],
                           'endpoint_type': i['Endpoint_Type'],
                           'flow': flow,
                           'flow_unit': i['Flow_Unit'],
                           'location_address_line1': i['Location_Address_Line1'],
                           'current_leak_rate': leak_rate,
                           'current_leak_start_date': leak_date,
                           'backflow_gallons': backflow,
                           'battery_level': i['Battery_Level'],
                           'service_point_route': int(s),
                           'flow_time': flow_time
                       }
                   }
            #this function edits the feature class or data table, specifically adding the value in the list
            #you MUST put the value you're adding inside brackets or else it throws an error
            data_table.edit_features(adds=[add])


#general method to access the model in gis
def access_model(g):
    # access the portal gis
    gis = GIS("https://esriapps1.esriwadc.com/portal", g['username'], g['password'])
    model_layer = gis.content.get('bba03d3af8b849848a9691b9042598be')


    # individual feature services contained in the hosted layer. There is one feature layer and one table
    geometry_layer = model_layer.layers[0]
    table_layer = model_layer.tables[0]

    return geometry_layer, table_layer, model_layer, gis


#general function to add a new point to the water meter model
def build_site(d, geometry_layer, table_layer):
    # conditional statements to check for null values and to convert the strings in the server response to their
    # appropriate data types
    try:
        flow = float(d['Flow'])
    except (ValueError, TypeError):
        flow = 0
    try:
        flow_time = dt.datetime.strptime(d['Flow_Time'], '%Y-%m')
    except (ValueError, TypeError):
        flow_time = None
    try:
        sn = int(d['Endpoint_SN'])
    except (ValueError, TypeError):
        sn = None

    # feature datastructure, includes attribute and geometry fields for water meters. spatial reference is
    # the geographic coordinate system well-known identifier for TN state plane
    if flow_time is not None:
        geo = {'attributes': {
            'account_full_name': d['Account_Full_Name'],
            'account_id': d['Account_ID'],
            'endpoint_sn': sn,
            'location_address': d['Location_Address_Line1'],
            'location_city': d['Location_City'],
            'sa_start_date': dt.datetime.strptime(d['SA_Start_Date'], '%Y-%m'),
            'read_method': d['Read_Method'],
            f'{(calendar.month_name[flow_time.month])}_gpm'.lower(): flow/(30.437*24*60)
        },
            'geometry': {
                'x': float(d['Service_Point_Longitude']),
                'y': float(d['Service_Point_Latitude']),
                'spatialReference': {'wkid': 6318}
            }
        }
        # table that stores the flow data, data is linked to geometry via the endpoint serial number
        table = {'attributes': {
            'endpoint_sn': sn,
            'flow': flow,
            'flow_time': flow_time
        }
        }
        # adds the datapoints to the feature layer
        geometry_layer.edit_features(adds=[geo])
        table_layer.edit_features(adds=[table])

        #correct the location to be in line with master layer, I found the difference in this data and the master was
        #basically the same for all the points so I took the difference in x and y and applied it to the geometry.
        #it seems to make the meters line up correctly so I will use this
        fset = geometry_layer.query(where=f'endpoint_sn = {sn}')
        f = fset.features
        f[0].geometry['x'] += 1.490687
        f[0].geometry['y'] += -2.50416324
        geometry_layer.edit_features(updates=[f[0]])


#edit the site attributes and add the table entry
def edit_site(d, geometry_layer, table_layer, feature):
    # conditional statements to check for null values and to convert the strings in the server response to their
    # appropriate data types
    try:
        flow = float(d['Flow'])
    except (ValueError, TypeError):
        flow = 0
    try:
        flow_time = dt.datetime.strptime(d['Flow_Time'], '%Y-%m')
        #unclear if this will work, this is just one idea to fix the problem
        feature.attributes[f'{calendar.month_name[flow_time.month]}_gpm'.lower()] = flow / (30.437 * 24 * 60)
    except (ValueError, TypeError):
        flow_time = None
    try:
        sn = int(d['Endpoint_SN'])
    except (ValueError, TypeError):
        sn = None

    # feature datastructure, includes attribute and geometry fields for water meters. spatial reference is
    # the geographic coordinate system well-known identifier for TN state plane

    feature.attributes['account_full_name'] = d['Account_Full_Name']
    feature.attributes['account_id'] = d['Account_ID']
    feature.attributes['endpoint_sn'] = sn
    feature.attributes['location_address'] = d['Location_Address_Line1']
    feature.attributes['location_city'] = d['Location_City']
    feature.attributes['sa_start_date'] = dt.datetime.strptime(d['SA_Start_Date'], '%Y-%m')

    #changes the read method to reflect whether flow was detected there or not
    if flow == 0:
        feature.attributes['read_method'] = f'Inactive-{d["Read_Method"]}'
    else:
        feature.attributes['read_method'] = d['Read_Method']

    #this is supposed to check if flow_time is null and enter data into the appropriate field if it's not
    #this does not work consistently, approximately 450 records did not record as intended for September
    # if flow_time is not None:
    #     feature.attributes[f'{calendar.month_name[flow_time.month]}_gpm'.lower()] = flow/(30.437*24*60)

    # table that stores the flow data, data is linked to geometry via the endpoint serial number
    table = {'attributes': {
        'endpoint_sn': sn,
        'flow': flow,
        'flow_time': flow_time
    }
    }
    # edits the fields in the feature layer and adds data to the table
    geometry_layer.edit_features(updates=[feature])
    table_layer.edit_features(adds=[table])


#calculate the monthly average gpm for data already entered
def monthly_average(gps):
    g, t, i, gis = access_model(gps)
    fset = g.query()
    feat = fset.features

    for f in feat:
        table = g.query_related_records(object_ids=f'{f.attributes["objectid"]}', relationship_id='0')
        # print(table)
        for tab in table['relatedRecordGroups'][0]['relatedRecords']:
            try:
                date = dt.datetime.fromtimestamp(tab['attributes']['flow_time']*10**-3).month
            except TypeError:
                continue
            # print(calendar.month_name[date])
            # print(tab['attributes']['flow']/(30.437*24*60))
            f.attributes[f'{calendar.month_name[date]}_gpm'.lower()] = tab['attributes']['flow']/(30.437*24*60)
        g.edit_features(updates=[f])


#calculate the water metering site's aggregated averages per Michael's request
def averages(gps):
    g, t, it, gis = access_model(gps)
    fset = g.query()
    feat = fset.features

    for f in feat:
        count_annual = 0
        count_summer = 0
        sum_annual = 0
        sum_summer = 0
        peak = 0
        #loop through the months
        for i, m in enumerate(calendar.month_name):
            if i == 0:
                continue
            #if the month is empty, skip it in the average
            if f.attributes[f'{m}_gpm'.lower()] is None:
                count_annual += 1
                if 7 <= i <= 9:
                    count_summer += 1
            else:
                sum_annual += f.attributes[f'{m}_gpm'.lower()]
                if f.attributes[f'{m}_gpm'.lower()] > peak:
                    peak = f.attributes[f'{m}_gpm'.lower()]
                if 7 <= i <= 9:
                    sum_summer += f.attributes[f'{m}_gpm'.lower()]
        try:
            avg_annual = sum_annual/(12-count_annual)
            avg_summer = sum_summer/(3-count_summer)
        except ZeroDivisionError:
            avg_summer = 0
            avg_annual = 0

        f.attributes['annual_avg'] = avg_annual
        f.attributes['summer_flow'] = avg_summer
        f.attributes['peak_flow'] = peak
        g.edit_features(updates=[f])


#function used to build the meter section of the water model in the gis
def build_model(data, gis):
    geometry_layer, table_layer, i, g = access_model(gis)

    for d in data[0]:
        build_site(d, geometry_layer, table_layer)


#resets the model values to None in the master model
def reset_model(geometry_layer, feature):
    for m in calendar.month_name:
        feature.attributes[f'{m}_gpm'.lower()] = None

    feature.attributes['peak_flow'] = None
    feature.attributes['annual_avg'] = None
    feature.attributes['summer_flow'] = None

    geometry_layer.edit_features(updates=[feature])


#function to update the water model now that it has been built, now include current month as argument to reset model
def update_model(data, current_month, gps):
    geometry_layer, table_layer, item, gis = access_model(gps)

    #before updating the water model, we want to archive the data we currently have
    if current_month == 3:
        #clone the current water model
        clone = gis.content.clone_items(items=[item], owner='wadc_engr03', folder='water_model_data')
        #update the title
        clone.update(item_properties={'title': f'Water Meter Model {current_month.year - 1}'})
        #shares the archived data with the archived water meter data group
        clone.share(groups=['f31fd74a860249cababe86578a48f536'])

    #loop through the data dictionary
    for d in data[0]:
        # service_start = dt.datetime.strptime(d['SA_Start_Date'], '%Y-%m')
        #if there is no serial number in the data row then the where clause is this
        if d['Endpoint_SN'] is None:
            where_clause = "location_address = '{}'".format(d['Location_Address_Line1'])
        else:
            #format the serial number and address
            sn = int(d['Endpoint_SN'])
            if "'" in d['Location_Address_Line1']:
                address = d['Location_Address_Line1'].replace("'", "''")
            else:
                address = d['Location_Address_Line1']

            #this could probably be a f-string but I was having trouble so I did this old way of making
            #dynamic strings

            #this where clause is used for the sql query to get features that match these conditions
            where_clause = "location_address = '{}' OR endpoint_sn = {}".format(address, sn)

        geo_fset = geometry_layer.query(where=where_clause)
        features = geo_fset.features

        #if the query returns nothing then that means there isn't a site with these properties so it makes a new one
        if not features:
            # print(d)
            build_site(d, geometry_layer, table_layer)
        #otherwise the site that is returned is edited and the edits are reflected in the hosted feature layer
        else:
            if current_month.month == 3:
                reset_model(geometry_layer, features[0])
            # print(features)
            edit_site(d, geometry_layer, table_layer, features[0])
