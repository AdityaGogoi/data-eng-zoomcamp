import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]



@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, help='Year of the data')
@click.option('--month', default=1, help='Month of the data')
@click.option('--chunksize', default=100000, help='Chunk size for ingestion')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month, chunksize):

    # target_table = 'yellow_taxi_data'
    # year = 2021
    # month = 1
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

    print('URL=', prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz')
    
    df = pd.read_csv(prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz')
    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # DDL Schema
    print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

    #Create iterator df object
    df_iter = pd.read_csv(
        prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # Complete ingestion loop
    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))



if __name__ == '__main__':
    run()




