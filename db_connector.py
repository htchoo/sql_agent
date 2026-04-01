import os
import urllib.parse
import psycopg2
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text  # text 모듈 추가
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

        # 2. SQLAlchemy용 연결 문자열 생성
        safe_password = urllib.parse.quote_plus(self.password) if self.password else ""
        # SQLAlchemy 버전에 따라 드라이버(postgresql+psycopg2)를 명시해주는 것이 안전합니다.
        self.sqlalchemy_url = f"postgresql+psycopg2://{self.user}:{safe_password}@{self.host}:{self.port}/{self.database}?sslmode=require"
        
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
        query = f"SELECT * FROM public.{table_name};"
        try:
            df = pd.read_sql(query, self.engine)
            return df
        except Exception as e:
            return None

    # --- [기능 수정 및 강화: upload_master_table] ---
    def upload_master_table(self, df, table_name, upload_mode='replace'):
        """
        사용자가 선택한 모드에 따라 테이블을 관리합니다.
        - 'replace': 기존 테이블 삭제 후 새로 생성 (스키마 변경 시)
        - 'delete_insert': 테이블 구조는 유지하고 데이터만 전체 교체
        - 'append': 기존 데이터 아래에 추가로 쌓기
        """
        try:
            target_table = table_name.lower()
            
            if upload_mode == 'delete_insert':
                # 1. TRUNCATE를 통해 데이터만 깔끔하게 비움
                with self.engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE public.{target_table}"))
                    conn.commit()
                # 2. 데이터 삽입 (기존 구조 활용)
                df.to_sql(name=target_table, con=self.engine, if_exists='append', index=False)
                msg = f"'{target_table}'의 데이터를 모두 비우고 새로 입력했습니다."
                
            elif upload_mode == 'append':
                # 기존 데이터에 추가
                df.to_sql(name=target_table, con=self.engine, if_exists='append', index=False)
                msg = f"'{target_table}' 테이블에 데이터를 추가(Append)했습니다."
                
            else: # 'replace' (기본값)
                # 테이블 삭제 후 재생성
                df.to_sql(name=target_table, con=self.engine, if_exists='replace', index=False)
                msg = f"'{target_table}' 테이블을 새로 생성하여 업로드했습니다."
                
            return True, msg
        except Exception as e:
            return False, f"업로드 중 오류 발생: {e}"