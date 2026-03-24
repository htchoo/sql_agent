import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class NeonDBConnector:
    def __init__(self):
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.database = os.getenv("PG_DATABASE")
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.conn_str = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} sslmode=require"

    def get_connection(self):
        try:
            return psycopg2.connect(self.conn_str)
        except Exception as e:
            raise Exception(f"DB 연결 실패: {e}")

    def get_all_tables(self):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tables
        except Exception as e:
            return []

    def get_table_schema(self, table_name):
        query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public'
            AND table_name = '{table_name}';
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            schema = cursor.fetchall()
            conn.close()
            return schema
        except Exception as e:
            return []

    def get_sample_data(self, table_name, limit=5):
        query = f"SELECT * FROM public.{table_name} LIMIT {limit};"
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            conn.close()
            return columns, data
        except Exception as e:
            return [], str(e)

    # [NEW] 매핑율 계산을 위해 마스터 테이블의 전체 데이터를 가져오는 함수
    def get_full_master_data(self, table_name):
        """조인 및 통계 산출을 위해 마스터 테이블 전체 데이터를 DataFrame으로 반환"""
        query = f"SELECT * FROM public.{table_name};"
        try:
            import pandas as pd
            conn = self.get_connection()
            # pandas의 read_sql을 사용하여 바로 DataFrame으로 변환
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            return None