# dags/clean_data.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_buildings_flats_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        from sqlalchemy import inspect, Table, MetaData, Column, Integer, String, Float, UniqueConstraint
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()
        
        # Drop table if it exists
        hook.run("DROP TABLE IF EXISTS buildings_flats_cleaned CASCADE")
        print(f"Старая таблица удалена")
        
        # Create table with unique constraint
        metadata = MetaData()
        buildings_flats_cleaned = Table(
            'buildings_flats_cleaned',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('building_id', Integer, nullable=False),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', String),
            Column('flat_id', Integer, nullable=False),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', String),
            Column('studio', String),
            Column('total_area', Float),
            Column('price', Float),
            UniqueConstraint('building_id', 'flat_id')
        )
        metadata.create_all(db_engine)
        print(f"Таблица 'buildings_flats_cleaned' создана с уникальным ограничением")
        

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""select * from buildings_flats_joined"""
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
    
        def remove_duplicates(df):

            return df
        
        def handle_outliers(df):
            num_cols = df.select_dtypes(include=['float64']).columns
            for col in num_cols:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                df = df[df[col].between(lower, upper)]
            return df
        
        def fill_missing(df):

            return df

        # Цепочка преобразований
        data = remove_duplicates(data)
        data = handle_outliers(data)
        data = fill_missing(data)

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
    
        # Полностью очищаем таблицу
        cursor.execute("TRUNCATE TABLE buildings_flats_cleaned")
        conn.commit()

        hook.insert_rows(
            table= 'buildings_flats_cleaned',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id', 'flat_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_buildings_flats_dataset()