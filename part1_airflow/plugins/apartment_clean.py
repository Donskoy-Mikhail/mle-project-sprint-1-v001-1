from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np


def create_table_clean():
    from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, UniqueConstraint, Float
    from sqlalchemy import inspect
# ваш код здесь тоже #
    postgres_hook = PostgresHook('destination_db')
    engine = postgres_hook.get_sqlalchemy_engine()
    metadata = MetaData()
    apartment_clean_table = Table(
        'apartment_clean_table',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('building_id', String, nullable=False),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', String),
        Column('studio', String),
        Column('total_area', Float),
        Column('price', Float, nullable=False),
        Column('build_year', Integer),
        Column('building_type_int', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', String))

    # Проверяем, существует ли таблица
    if not inspect(engine).has_table(apartment_clean_table.name):
        # Создаём таблицу, если её нет
        metadata.create_all(engine)
    
def extract_raw(**kwargs):
        """
        #### Extract task
        """
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = """
                SELECT *
                FROM apartment_raw_table
                """
        data = pd.read_sql(sql, conn)
        ti = kwargs['ti']
        ti.xcom_push('extracted_raw_data', data)
        conn.close()

def transform_raw(**kwargs):
    """
    #### Transform task
    """
    has_outliers = ['price', 'living_area', 'total_area', 'ceiling_height',
                    'floors_total', 'longitude','latitude']

    def remove_duplicates(data):
        feature_cols = data.columns.drop('id').tolist()
        is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
        data = data[~is_duplicated_features].reset_index(drop=True)
        return data

    def remove_outliers(data, num_cols, threshold = 1.5):
        threshold = 1.5
        potential_outliers = pd.DataFrame()
        
        for col in num_cols:
            Q1 = data[col].quantile(0.25)
            Q3 = data[col].quantile(0.75)
            IQR = Q3 - Q1
            margin = threshold * IQR
            lower = Q1 - margin
            upper = Q3 + margin
            potential_outliers[col] = ~data[col].between(lower, upper)
        
        outliers = potential_outliers.any(axis=1)
        return data[~outliers]

    ti = kwargs['ti'] 
    data = ti.xcom_pull(task_ids='extract_raw', key='extracted_raw_data') 

    data = remove_duplicates(data)
    data = remove_outliers(data, has_outliers)

    ti.xcom_push('transformed_data', data) 

def load_clean(**kwargs):
    """
    #### Load task
    """
    hook = PostgresHook('destination_db')
    ti = kwargs['ti'] 
    data = ti.xcom_pull(task_ids='transform_raw', key='transformed_data')
    hook.insert_rows(
        table="apartment_clean_table",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist())