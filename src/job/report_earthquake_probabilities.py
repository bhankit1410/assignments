# python3

import requests
import pandas as pd
import seaborn as sns

from util.mysql import setup_mysql
from common.constants import DATABASE, TABLE

_CM = sns.light_palette("lightgreen", as_cmap=True)
_COLUMNS = "(event_id, place, mag, time, longitude, latitude, depth)"


def _convert_epoch_millis(millis):
    hours=(millis/(1000*60*60))%24
    return int(hours)


def _prepare_urls():
    urls = {}
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11']
    for month in months:
        start_time = f'2017-{month}-01'
        end_time = f'2017-{str(int(month) + 1)}-01'
        urls[f"{start_time} to {end_time}"] = \
            f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"

    urls[f"2017-12-01 to 2018-01-01"] = \
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2017-12-01&endtime=2018-01-01"
    return urls


def _extract_load(urls, db_conn):
    sql_cursor = db_conn.cursor()
    for date_range, url in urls.items():
        sql = f"INSERT INTO {TABLE} {_COLUMNS} VALUES (%s, %s, %s, %s, %s, %s, %s)"
        print(f"fetching the data for {date_range} from {url}")
        payload = requests.get(url).json()
        val = []
        for feature in payload['features']:
            event_id = feature['id']
            mag = feature['properties']['mag']
            epoch_time = feature['properties']['time']
            place = feature['properties']['place']
            coordinates = feature['geometry']['coordinates']
            longitude = coordinates[0]
            latitude = coordinates[1]
            depth = coordinates[2]

            val.append((event_id, place, mag, epoch_time, longitude, latitude, depth))

        print(f"loading the data for {date_range} into mysql <db:{DATABASE}, table:{TABLE}")
        sql_cursor.executemany(sql, val)
        db_conn.commit()


def _build_report(df):
    for mag_range in df['rounded_magnitude'].unique():
        df[df['rounded_magnitude'] == mag_range].plot(x='hour', y='count')


if __name__ == '__main__':
    # setup mysql
    db_connection = setup_mysql()
    cursor = db_connection.cursor()

    # Create a list of urls for each month to distribute the load. Could be loaded to RDBMS in parallel if needed.
    urls_list = _prepare_urls()

    # load data to mysql table.
    _extract_load(urls_list, db_connection)

    # Loading the data and analysis could be a different de-coupled job

    cursor.execute(f'SELECT * FROM {TABLE}')
    table_rows = cursor.fetchall()
    df = pd.DataFrame(table_rows, columns=cursor.column_names).set_index('id')

    # Transformations:
    # 1. filter out the records without a magnitude.
    # 2. extract the needed information.

    filtered_df = df[df.mag.notnull()][df['mag'] >= 0]
    filtered_df["hour"] = [_convert_epoch_millis(datetime) for datetime in (filtered_df["time"]/1000)]
    filtered_df["rounded_magnitude"] = [f'{int(mag)} - {int(mag) + 1}' if mag < 6 else ">6" for mag in filtered_df["mag"]]

    # query 5:
    strongest_earthquake = filtered_df.query('mag == mag.max()').iloc[0]

    print(strongest_earthquake)

    # query 6:
    agg = filtered_df.groupby(['rounded_magnitude', 'hour'])['time'].count().reset_index(name="count")
    agg['rank'] = agg.groupby(['rounded_magnitude'])['count'].rank(method='first', ascending=False)
    hour_for_earthquakes = agg[agg['rank'] == 1.0]
    # show the result for query
    print(hour_for_earthquakes.head(10))

    # plot a graph.
    _build_report(agg)

    db_connection.close()
