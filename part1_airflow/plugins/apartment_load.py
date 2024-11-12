from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np


def create_table():
    from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, UniqueConstraint, Float
    from sqlalchemy import inspect
# ваш код здесь тоже #
    postgres_hook = PostgresHook('destination_db')
    engine = postgres_hook.get_sqlalchemy_engine()
    metadata = MetaData()
    apartment_raw_table = Table(
        'apartment_raw_table',
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
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', String))

    # Проверяем, существует ли таблица
    if not inspect(engine).has_table(apartment_raw_table.name):
        # Создаём таблицу, если её нет
        metadata.create_all(engine)
    
def extract(**kwargs):
        """
        #### Extract task
        """
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = """
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
                    f.id AS id,
                    f.floor,
                    f.kitchen_area,
                    f.living_area,
                    f.rooms,
                    f.is_apartment,
                    f.studio,
                    f.total_area,
                    f.price
                FROM 
                    buildings b
                JOIN 
                    flats f ON b.id = f.building_id;
                """
        data = pd.read_sql(sql, conn)
        ti = kwargs['ti']
        ti.xcom_push('extracted_data', data)
        conn.close()

def load(**kwargs):
    """
    #### Load task
    """
    hook = PostgresHook('destination_db')
    ti = kwargs['ti'] 
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    hook.insert_rows(
        table="apartment_raw_table",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist())