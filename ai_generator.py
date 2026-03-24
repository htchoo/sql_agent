import os
import json
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate

load_dotenv()

class SQLAgent:
    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(".env 파일에 OPENAI_API_KEY가 설정되지 않았습니다.")

        self.llm = ChatOpenAI(
            model="gpt-4o",      
            temperature=0,       
            openai_api_key=api_key,
            model_kwargs={"response_format": {"type": "json_object"}}
        )

    def generate_query_and_code(self, excel_samples, target_col, master_table, master_col, master_schema, master_samples):
        
        prompt = PromptTemplate(
            input_variables=["excel_samples", "target_col", "master_table", "master_col", "master_schema", "master_samples"],
            template="""
            당신은 Google BigQuery SQL Expert입니다.
            사용자가 정의한 **"3단계 우선순위 매핑 로직(3-Step Waterfall)"**을 준수하는 쿼리를 작성하세요.

            [매핑 로직 정의 (Priority Order)]
            **공통:** 조인 시 Source와 Master 컬럼 모두 **대문자(UPPER)**로 변환하여 비교한다.
            
            1. **Step 1 (우선순위 1등):** 공백과 특수문자를 모두 제거하고 매핑 시도.
               - Logic: `REGEXP_REPLACE(UPPER(col), r'[^a-zA-Z0-9]', '')`
            2. **Step 2 (우선순위 2등):** Step 1이 실패하면, 문자 끝에 `(I)`를 붙여서 매핑 시도.
               - Logic: `CONCAT(TRIM(UPPER(col)), '(I)')`
            3. **Step 3 (우선순위 3등):** Step 2도 실패하면, 문자 끝에 `(IPL)`을 붙여서 매핑 시도.
               - Logic: `CONCAT(TRIM(UPPER(col)), '(IPL)')`

            [데이터 정보]
            - Source: `{target_col}` (Sample: {excel_samples})
            - Master: `{master_table}`.`{master_col}`

            [작성 요구사항]
            1. **`sql_query`**: 
               - `LEFT JOIN`을 3번 사용하거나, 복잡한 조건을 처리하기 위해 `LEFT JOIN`과 `COALESCE`를 사용하세요.
               - **최종 결과 컬럼(`cleaned_key`)**: 매핑에 성공한 마스터 테이블의 Key 값, 혹은 매핑된 로직으로 변환된 Source 값을 출력하세요.
               - 매핑되지 않은 경우 NULL 또는 원본을 출력하세요.
            
            2. **`python_code`**: (이 항목은 이번 로직이 고정되어 있으므로 빈 문자열로 반환해도 됩니다.)

            [출력 JSON 형식]
            {{
                "sql_query": "SELECT S.{target_col} as origin, COALESCE(m1.{master_col}, m2.{master_col}, m3.{master_col}) as final_key ... FROM source S LEFT JOIN master m1 ON ... LEFT JOIN master m2 ON ... LEFT JOIN master m3 ON ...",
                "python_code": ""
            }}
            """
        )

        final_prompt = prompt.format(
            excel_samples=str(excel_samples),
            target_col=target_col,
            master_table=master_table,
            master_col=master_col,
            master_schema=str(master_schema),
            master_samples=str(master_samples)
        )

        response = self.llm.invoke(final_prompt)
        return json.loads(response.content)