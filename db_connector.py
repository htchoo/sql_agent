import os
import urllib.parse
import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

class NeonDBConnector:
    def __init__(self):
        # 로컬 환경(.env)과 Streamlit Cloud(st.secrets) 모두 호환되도록 변수 불러오기
        def get_env_var(var_name):
            try:
                return st.secrets[var_name]
            except:
                return os.getenv(var_name)

        self.host = get_env_var("PG_HOST")
        self.port = get_env_var("PG_PORT")
        self.database = get_env_var("PG_DATABASE")
        self.user = get_env_var("PG_USER")
        self.password = get_env_var("PG_PASSWORD")

        # 1. psycopg2용 연결 문자열 (기존 방식 유지)
        self.conn_str = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} sslmode=require"

        # 2. SQLAlchemy용 연결 문자열 생성 (to_sql을 위한 엔진용)
        # 비밀번호에 특수문자가 있을 경우를 대비해 안전하게 인코딩 처리
        safe_password = urllib.parse.quote_plus(self.password) if self.password else ""
        self.sqlalchemy_url = f"postgresql://{self.user}:{safe_password}@{self.host}:{self.port}/{self.database}?sslmode=require"
        
        # 데이터프레임 업로드를 위한 엔진 생성
        self.engine = create_engine(self.sqlalchemy_url)

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

    def get_full_master_data(self, table_name):
        """조인 및 통계 산출을 위해 마스터 테이블 전체 데이터를 DataFrame으로 반환"""
        query = f"SELECT * FROM public.{table_name};"
        try:
            # 경고 메시지 방지를 위해 psycopg2 커넥션 대신 sqlalchemy 엔진을 사용하도록 개선
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            return None

    def upload_master_table(self, df, table_name):
        """데이터프레임을 DB의 새로운 마스터 테이블로 저장합니다."""
        try:
            # SQLAlchemy 엔진(self.engine)을 사용하여 DB에 테이블 생성 및 데이터 삽입
            df.to_sql(name=table_name.lower(), con=self.engine, if_exists='replace', index=False)
            return True, f"'{table_name.lower()}' 테이블이 성공적으로 생성/업데이트 되었습니다."
        except Exception as e:
            return False, f"테이블 생성 중 오류가 발생했습니다: {e}"