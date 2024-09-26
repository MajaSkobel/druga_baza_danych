import sqlalchemy
import csv
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, Float, ForeignKey, MetaData

meta = MetaData()

stations = Table(
    'stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String, unique=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)

measures = Table(
    'measures', meta,
    Column('id', Integer, primary_key=True),
    Column('station_name', String, ForeignKey('stations.station')),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
)

def insert(conn,table,input_values):
    ins = table.insert().values(input_values)
    conn.execute(ins)

def delete(conn,table,condition):
    delet = table.delete().where(condition)
    conn.execute(delet)

def select_and_show(conn,table,condition):
    select = table.select().where(condition)
    result = conn.execute(select)
    for row in result:
        print(row)

if __name__ == '__main__':
    
    engine = create_engine('sqlite:///weather_data.db', echo=True)
    conn=engine.connect()
    meta.create_all(engine)

    txt_file = r"clean_stations.txt"
    with open(txt_file, 'r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            station_data = {
                'station': row['station'],
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude']),
                'elevation': float(row['elevation']),
                'name': row['name'],
                'country': row['country'],
                'state': row['state'],
            }
            insert_station = stations.insert().values(station_data)
            conn.execute(insert_station)

    txt_file = r"clean_measure.txt"
    with open(txt_file, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            measure_data = {
                'station_name': row['station'],
                'date': row['date'],
                'precip': float(row['precip']),
                'tobs': int(row['tobs']),
            }
            insert_measure = measures.insert().values(measure_data)
            conn.execute(insert_measure)

    print("Przeprowadzamy kilka testów!:")

    print("Najpierw dodajmy jakieś dane:")
    measure_data = {
        'station_name':'USC00519281',
        'date':'2024-07-06',
        'precip':0.01,
        'tobs':70,
    }
    insert(conn,measures,measure_data)

    print("Teraz usuńmy coś:")
    delete(conn,measures,measures.c.station_name == 'USC00516128')

    print("A na koniec coś wybierzmy i pokażmy co wybraliśmy:")
    select_and_show(conn,stations,stations.c.latitude > 21.4 ) 