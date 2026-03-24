import streamlit as st
import pandas as pd
import re
import difflib
import io  # 텍스트 데이터를 읽기 위해 추가된 모듈
from db_connector import NeonDBConnector

# 1. 페이지 설정
st.set_page_config(page_title="BigQuery Multi-Column Mapper", layout="wide")

# ==========================================
# 🔐 보안 모듈: 설정된 암호를 아는 사람만 접근 가능
# ==========================================
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_ACCESS_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"] 
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.title("🔒 SQL Agent 보안 접속")
        st.text_input("접근 암호를 입력하세요 (Access Key)", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.title("🔒 SQL Agent 보안 접속")
        st.text_input("접근 암호를 입력하세요 (Access Key)", type="password", on_change=password_entered, key="password")
        st.error("😕 암호가 올바르지 않습니다.")
        return False
    return True

if not check_password():
    st.stop()

# ==========================================
# 🚀 메인 앱 로직
# ==========================================
st.title("🧩 통합 매핑 분석 및 시계열 전처리 에이전트")

# 하드코딩 룰
DIV_SPECIAL_RULES = {
    "AV": "Audio", "CTV": "Commercial TV", "DS": "DS (Brand)",
    "ESS": "ESS BD", "LIGHTING": "Smart Lighting", "MOTOR": "Motor BD",
    "RAC": "RAC BD", "VACUUM CLEANER": "VCC", "WATER PURIFIER": "Water Care"
}

# DB 커넥터 로드
try:
    db = NeonDBConnector()
except Exception as e:
    st.error(f"DB 커넥터 로드 실패: {e}")
    st.stop()

# --- [유틸리티 함수 1: 매핑 로직] ---
def apply_hybrid_matching(val, m_table, m_raw_map, m_clean_map, sorted_suffixes, similarity_threshold=0.75):
    if pd.isna(val): return None
    val_str, val_upper = str(val).strip(), str(val).strip().upper()
    
    if m_table == "div_info_m" and val_upper in DIV_SPECIAL_RULES:
        rule_val = DIV_SPECIAL_RULES[val_upper]
        if rule_val.upper() in m_raw_map: return m_raw_map[rule_val.upper()]
    
    val_clean = re.sub(r'[^a-zA-Z0-9]', '', val_upper)
    if val_clean in m_clean_map: return m_clean_map[val_clean]
    
    for s in sorted_suffixes:
        clean_s = re.sub(r'[^a-zA-Z0-9]', '', str(s).upper())
        if (val_clean + clean_s) in m_clean_map: return m_clean_map[val_clean + clean_s]
    
    master_list = list(m_raw_map.values())
    matches = difflib.get_close_matches(val_str, master_list, n=1, cutoff=similarity_threshold)
    return matches[0] if matches else None

# --- [유틸리티 함수 2: 시계열 분석] ---
def analyze_date_periodicity(series):
    try:
        valid_series = series.dropna()
        if valid_series.empty: return None, None, None, None
        sample_val = str(valid_series.iloc[0])
        if len(sample_val) == 8 and sample_val.isdigit():
            dates = pd.to_datetime(valid_series.astype(str), format='%Y%m%d', errors='coerce').dropna().unique()
        else:
            dates = pd.to_datetime(valid_series, errors='coerce').dropna().unique()
            
        dates = sorted(dates)
        if len(dates) < 2: return "분석 불가", 0, {}, None
        
        df_diff = pd.DataFrame({"previous": dates[:-1], "current": dates[1:]})
        df_diff['gap'] = (df_diff['current'] - df_diff['previous']).dt.days
        
        avg_diff = df_diff['gap'].mean()
        gap_counts = df_diff['gap'].value_counts()
        main_gap = gap_counts.idxmax()
        outliers = df_diff[df_diff['gap'] != main_gap].copy()
        
        if df_diff['gap'].std() < 1.5:
            if 0.8 <= avg_diff <= 1.2: desc = "📅 일별(Daily)"
            elif 6.5 <= avg_diff <= 7.5: desc = "📅 주별(Weekly)"
            elif 27 <= avg_diff <= 32: desc = "📅 월별(Monthly)"
            else: desc = f"📅 고정 주기({avg_diff:.1f}일)"
        else:
            desc = f"⚠️ 불규칙 (평균 {avg_diff:.1f}일)"
            
        return desc, avg_diff, gap_counts.to_dict(), outliers
    except:
        return None, None, None, None

# --- [UI: 데이터 입력 구역 (수정된 부분)] ---
st.subheader("📥 데이터 입력")
source_df = None

# 파일 업로드 방식과 복사/붙여넣기 방식을 탭으로 분리
tab1, tab2 = st.tabs(["📋 직접 붙여넣기 (권장)", "📁 파일 업로드"])

with tab1:
    st.markdown("엑셀에서 데이터를 표기할 **컬럼 헤더를 포함하여** 드래그 후 복사(Ctrl+C)하고 아래 빈칸에 붙여넣기(Ctrl+V) 하세요.")
    pasted_text = st.text_area("데이터 붙여넣기:", height=200, placeholder="여기에 데이터를 붙여넣으세요...")
    
    if pasted_text.strip():
        try:
            # 엑셀에서 복사한 데이터는 탭(\t)으로 구분되므로 이를 파싱합니다.
            source_df = pd.read_csv(io.StringIO(pasted_text), sep='\t')
        except Exception as e:
            st.error(f"데이터를 읽는 중 오류가 발생했습니다: {e}")

with tab2:
    uploaded_file = st.file_uploader("전처리할 엑셀 파일을 업로드하세요", type=['xlsx', 'csv'])
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                source_df = pd.read_csv(uploaded_file)
            else:
                source_df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

# --- [이하 데이터 처리 로직은 기존과 동일] ---
if source_df is not None and not source_df.empty:
    st.success("데이터가 성공적으로 로드되었습니다!")
    st.subheader("📋 원본 데이터 프리뷰")
    st.dataframe(source_df.head(3), use_container_width=True)
    
    target_cols = st.multiselect("전처리 대상 컬럼 선택", source_df.columns)

    if target_cols:
        st.divider()
        st.subheader("⚙️ 컬럼별 매핑 및 분석 설정")
        all_tables = db.get_all_tables()
        col_mapping_config = {}
        config_cols = st.columns(len(target_cols))
        
        for i, col in enumerate(target_cols):
            with config_cols[i]:
                st.markdown(f"#### 📍 `{col}`")
                def_idx = all_tables.index("clndr_m") if "clndr" in str(col).lower() and "clndr_m" in all_tables else 0
                m_table = st.selectbox("마스터 테이블", all_tables, index=def_idx, key=f"t_{col}")
                m_cols, _ = db.get_sample_data(m_table)
                m_key = st.selectbox("기준 키", m_cols, key=f"k_{col}")
                col_mapping_config[col] = {"table": m_table, "key": m_key, "threshold": st.slider("유사도 민감도", 0.0, 1.0, 0.75, key=f"s_{col}")}

        if st.button("🚀 분석 실행", type="primary"):
            final_df, sql_parts, col_results = source_df.copy(), [], {}

            with st.spinner("데이터 분석 중..."):
                for col in target_cols:
                    config = col_mapping_config[col]
                    df_master = db.get_full_master_data(config['table'])
                    raw_map = {str(k).upper().strip(): k for k in df_master[config['key']]}
                    clean_map = {re.sub(r'[^a-zA-Z0-9]', '', str(k).upper()): k for k in df_master[config['key']]}
                    suffixes = sorted(list({str(k)[5:] for k in raw_map.keys() if len(str(k)) > 5}), key=len)
                    
                    mapped_name = f"{col}_MAPPED"
                    final_df[mapped_name] = final_df[col].apply(lambda x: apply_hybrid_matching(x, config['table'], raw_map, clean_map, suffixes, config['threshold']))
                    
                    p_desc, avg_gap, gap_dist, outliers = analyze_date_periodicity(final_df[col])
                    success_df = final_df[final_df[mapped_name].notnull()]
                    distinct_map = success_df[[col, mapped_name]].drop_duplicates()
                    
                    col_results[col] = {"rate": (len(success_df)/len(final_df))*100, "p_desc": p_desc, "avg_gap": avg_gap, "gap_dist": gap_dist, "outliers": outliers, "success_list": distinct_map, "fail_list": final_df[final_df[mapped_name].isnull()][[col]].drop_duplicates()}
                    
                    case_lines = []
                    for _, row in distinct_map.iterrows():
                        src_val, dst_val = str(row[col]), str(row[mapped_name])
                        if src_val != dst_val:
                            safe_src, safe_dst = src_val.replace("'", "''"), dst_val.replace("'", "''")
                            case_lines.append(f"    WHEN {col} = '{safe_src}' THEN '{safe_dst}'")
                    
                    sql_parts.append(f"  CASE\n" + "\n".join(case_lines) + f"\n    ELSE {col}\n  END AS {col}_CLEANED" if case_lines else f"  {col} AS {col}_CLEANED")

            # --- 결과 리포트 출력 ---
            st.success("✅ 매핑 및 분석 완료!")
            for col in target_cols:
                res = col_results[col]
                with st.container():
                    st.markdown(f"### 📊 `{col}` 분석 결과")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("매핑 성공률", f"{res['rate']:.1f}%")
                    if res['p_desc']:
                        m2.metric("데이터 주기성", res['p_desc'])
                        m3.metric("평균 날짜 간격", f"{res['avg_gap']:.1f}일")
                        
                        st.write("**📅 날짜 간격 분포 및 특이 데이터**")
                        g1, g2 = st.columns([2, 1])
                        g1.bar_chart(res['gap_dist'])
                        with g2:
                            if res['outliers'] is not None and not res['outliers'].empty:
                                st.warning(f"특이 간격 발견 ({len(res['outliers'])}건)")
                                st.dataframe(res['outliers'], hide_index=True)
                            else:
                                st.success("모든 간격이 일정합니다.")
                    st.divider()

            st.subheader("📝 최종 전처리 SQL (BigQuery)")
            full_sql = "SELECT\n  *,\n" + ",\n".join(sql_parts) + "\nFROM `데이터셋.테이블`"
            st.code(full_sql, language="sql")