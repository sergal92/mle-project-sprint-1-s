# dags/data_prep.py

import pendulum
from airflow.decorators import dag, task
from messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def prepare_buildings_flats_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    #ваш код здесь #
    @task()
    def create_table():
        from sqlalchemy import inspect, Table, MetaData, Column, Integer, String, Float, DateTime, UniqueConstraint # дополните импорты необходимых типов колонок
        # Используем PostgresHook для подключения к БД
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        
        metadata = MetaData()
        buildings_flats_joined = Table(
            'buildings_flats_joined',
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
            UniqueConstraint('building_id', 'flat_id', name='unique_building_flat')
        )
        
        # Проверяем существование таблицы и создаём если её нет
        if not inspect(engine).has_table(buildings_flats_joined.name):
            metadata.create_all(engine)
            print(f"Таблица '{buildings_flats_joined.name}' успешно создана")
        else:
            print(f"Таблица '{buildings_flats_joined.name}' уже существует")
        
        #return engine
    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
                SELECT 
                    b.id AS building_id,
                    b.build_year,
                    b.building_type_int,
                    b.latitude,
                    b.longitude,
                    b.ceiling_height,
                    b.flats_count,
                    b.floors_total,
                    b.has_elevator,
                    f.id AS flat_id,
                    f.floor,
                    f.kitchen_area,
                    f.living_area,
                    f.rooms,
                    f.is_apartment,
                    f.studio,
                    f.total_area,
                    f.price
                FROM buildings b
                INNER JOIN flats f ON b.id = f.building_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="buildings_flats_joined",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['building_id', 'flat_id'],
            rows=data.values.tolist()
    )

        # ваш код здесь #
    create_table()    
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_buildings_flats_dataset()