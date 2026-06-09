# dags/clean_data.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    @task()
    def create_table():
        from sqlalchemy import inspect, Table, MetaData, Column, Integer, String, Float, DateTime, UniqueConstraint # дополните импорты необходимых типов колонок
        # Используем PostgresHook для подключения к БД
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        
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
            UniqueConstraint('building_id', 'flat_id', name='unique_building_flat')
        )
        
        # Проверяем существование таблицы и создаём если её нет
        if not inspect(engine).has_table(buildings_flats_cleaned.name):
            metadata.create_all(engine)
            print(f"Таблица '{buildings_flats_cleaned.name}' успешно создана")
        else:
            print(f"Таблица '{buildings_flats_cleaned.name}' уже существует")
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
            feature_cols = df.columns.drop('customer_id').tolist()
            is_duplicated = df.duplicated(subset=feature_cols, keep=False)
            return df[~is_duplicated].reset_index(drop=True)
        
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
            cols_with_nans = df.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0].index
            for col in cols_with_nans:
                if col == 'end_date':
                    continue
                if df[col].dtype in ['float64', 'int64']:
                    df[col] = df[col].fillna(df[col].mean())
                elif df[col].dtype == 'object':
                    mode_val = df[col].mode()
                    if not mode_val.empty:
                        df[col] = df[col].fillna(mode_val.iloc[0])
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
        cursor.execute("TRUNCATE TABLE clean_users_churn")
        conn.commit()



        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        hook.insert_rows(
            table= 'clean_users_churn',
            replace=False,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()