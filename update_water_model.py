#author: Cole Howell
#Purpose: Automatically update the water meter model feature layer with new flow data and new meters


import beacon_api_functions as bapi
import datetime as dt


def main():
    # s_time = dt.datetime(2023, 1, 1)
    # e_time = dt.datetime(2023, 7, 31)
    num_months = [None, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    #sets the start and end times to the whole month two months ago since that's the last period of full data
    current_month = dt.datetime.now().replace(day=1, hour=0, minute=0, second=0)
    last_month = (current_month - dt.timedelta(days=1)).replace(day=1)
    # month_before = (last_month - dt.timedelta(days=1)).replace(day=1)
    if current_month.month == 1 or current_month.month == 2:
        s_time = current_month.replace(year=current_month.year - 1 ,month=num_months[current_month.month-3], day=1, hour=0, minute=0, second=0)
    else:
        s_time = current_month.replace(month=num_months[current_month.month - 2], day=1, hour=0, minute=0, second=0)
    e_time = (last_month - dt.timedelta(days=1)).replace(hour=23, minute=59, second=59)
    # e_time = (month_before - dt.timedelta(days=1)).replace(hour=23, minute=59, second=59)

    #configure file with login information
    g, b = bapi.config()
    #collect data
    data = bapi.monthly_audit(s_time, e_time, g)
    #update the model on gis with new metering locations if applicable
    bapi.update_model(data, current_month, g)
    #average flow data per metering location
    bapi.averages(g)


if __name__ == '__main__':
    main()
