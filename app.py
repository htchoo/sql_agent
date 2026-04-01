import streamlit as st
import pandas as pd
import re
import difflib
import io
from db_connector import NeonDBConnector

# 1. 페이지 설정
st.set_page_config(page_title="BigQuery Multi-Column Mapper", layout="wide")

# ==========================================
# 🔐 보안 모듈
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
# 🚀 전역 설정 및 DB 커넥터 로드
# ==========================================
DIV_SPECIAL_RULES = {
    "AV": "Audio", "CTV": "Commercial TV", "DS": "DS (Brand)",
    "ESS": "ESS BD", "LIGHTING": "Smart Lighting", "MOTOR": "Motor BD",
    "RAC": "RAC BD", "VACUUM CLEANER": "VCC", "WATER PURIFIER": "Water Care"
}

try:
    db = NeonDBConnector()
except Exception as e:
    st.error(f"DB 커넥터 로드 실패: {e}")
    st.stop()

# --- [유틸리티 함수] ---
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
    matches = difflib.get_close_matches(val_str, list(m_raw_map.values()), n=1, cutoff=similarity_threshold)
    return matches[0] if matches else None

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
        avg_diff, gap_counts = df_diff['gap'].mean(), df_diff['gap'].value_counts()
        outliers = df_diff[df_diff['gap'] != gap_counts.idxmax()].copy()
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

def find_best_master_table(col_name, all_tables):
    col_lower = str(col_name).lower()
    for i, t in enumerate(all_tables):
        t_lower = t.lower()
        if t_lower == f"{col_lower}_info_m" or t_lower == f"{col_lower}_m" or t_lower == col_lower:
            return i
    first_word = col_lower.split('_')[0]
    for i, t in enumerate(all_tables):
        if first_word in t.lower():
            return i
    matches = difflib.get_close_matches(col_lower, [t.lower() for t in all_tables], n=1, cutoff=0.4)
    if matches:
        return [t.lower() for t in all_tables].index(matches[0])
    return 0

def find_best_key_column(col_name, m_cols):
    col_lower = str(col_name).lower()
    m_cols_lower = [str(c).lower() for c in m_cols]
    if col_lower in m_cols_lower:
        return m_cols_lower.index(col_lower)
    matches = difflib.get_close_matches(col_lower, m_cols_lower, n=1, cutoff=0.4)
    if matches:
        return m_cols_lower.index(matches[0])
    return 0

# ==========================================
# 🗂️ 사이드바 메뉴 설정
# ==========================================
st.sidebar.title("📌 메뉴")
menu = st.sidebar.radio("작업을 선택하세요", ["데이터 전처리", "마스터 테이블 관리"])

# ==========================================
# 🟢 메뉴 1: 데이터 전처리 (기존 로직)
# ==========================================
if menu == "데이터 전처리":
    st.title("🧩 데이터 분석 및 전처리")
    st.subheader("📥 데이터 입력")

    if 'source_df' not in st.session_state:
        st.session_state['source_df'] = None

    tab1, tab2 = st.tabs(["📋 직접 붙여넣기 (권장)", "📁 파일 업로드"])

    with tab1:
        st.markdown("엑셀에서 데이터와 **컬럼 헤더**를 복사(Ctrl+C)하여 붙여넣기(Ctrl+V) 하세요.")
        pasted_text = st.text_area("데이터 영역:", height=200, placeholder="여기에 데이터를 붙여넣으세요...")
        
        if st.button("데이터 적용하기", key="btn_paste", type="primary"):
            if pasted_text.strip():
                try:
                    st.session_state['source_df'] = pd.read_csv(io.StringIO(pasted_text), sep='\t')
                except Exception as e:
                    st.error(f"데이터를 읽는 중 오류가 발생했습니다: {e}")

    with tab2:
        uploaded_file = st.file_uploader("전처리할 엑셀 파일을 업로드하세요", type=['xlsx', 'csv'], key="uf_main")
        if st.button("파일 적용하기", key="btn_upload", type="primary"):
            if uploaded_file:
                try:
                    if uploaded_file.name.endswith('.csv'):
                        st.session_state['source_df'] = pd.read_csv(uploaded_file)
                    else:
                        st.session_state['source_df'] = pd.read_excel(uploaded_file)
                except Exception as e:
                    st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    source_df = st.session_state['source_df']

    if source_df is not None and not source_df.empty:
        st.success("데이터가 성공적으로 로드되었습니다!")
        st.subheader("📋 원본 데이터 프리뷰")
        st.dataframe(source_df.head(3), use_container_width=True)
        
        target_cols = st.multiselect("전처리 대상 컬럼 선택", source_df.columns)

        if target_cols:
            st.divider()
            st.subheader("⚙️ 컬럼별 매핑 및 분석 설정")
            db.get_all_tables.clear() if hasattr(db.get_all_tables, 'clear') else None 
            all_tables = db.get_all_tables()
            
            col_mapping_config = {}
            config_cols = st.columns(len(target_cols))
            
            for i, col in enumerate(target_cols):
                with config_cols[i]:
                    st.markdown(f"#### 📍 `{col}`")
                    def_tab_idx = find_best_master_table(col, all_tables)
                    m_table = st.selectbox("마스터 테이블", all_tables, index=def_tab_idx, key=f"t_{col}")
                    m_cols, _ = db.get_sample_data(m_table)
                    def_key_idx = find_best_key_column(col, m_cols)
                    m_key = st.selectbox("기준 키", m_cols, index=def_key_idx, key=f"k_{col}")
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
                        
                        col_results[col] = {
                            "rate": (len(success_df)/len(final_df))*100 if len(final_df) > 0 else 0,
                            "p_desc": p_desc, "avg_gap": avg_gap, "gap_dist": gap_dist, "outliers": outliers, 
                            "success_list": distinct_map, 
                            "fail_list": final_df[final_df[mapped_name].isnull()][[col]].drop_duplicates(),
                            "master_table": config['table']
                        }
                        
                        case_lines = []
                        for _, row in distinct_map.iterrows():
                            src_val, dst_val = str(row[col]), str(row[mapped_name])
                            if src_val != dst_val:
                                safe_src, safe_dst = src_val.replace("'", "''"), dst_val.replace("'", "''")
                                case_lines.append(f"    WHEN {col} = '{safe_src}' THEN '{safe_dst}'")
                        
                        sql_parts.append(f"  CASE\n" + "\n".join(case_lines) + f"\n    ELSE {col}\n  END AS {col}_CLEANED" if case_lines else f"  {col} AS {col}_CLEANED")

                st.success("✅ 매핑 및 분석 완료!")
                for col in target_cols:
                    res = col_results[col]
                    with st.container():
                        st.markdown(f"### 📊 컬럼 분석 결과: `{col}` (Master: {res['master_table']})")
                        m1, m2, m3 = st.columns(3)
                        m1.metric("매핑 성공률", f"{res['rate']:.1f}%")
                        if res['p_desc'] and res['p_desc'] != "분석 불가":
                            m2.metric("데이터 주기성", res['p_desc'])
                            m3.metric("평균 날짜 간격", f"{res['avg_gap']:.1f}일")
                        c_success, c_fail = st.columns(2)
                        with c_success:
                            st.markdown(f"✅ <span style='color:green;font-size:14px'>매핑 성공 (총 {len(res['success_list'])}개 유형)</span>", unsafe_allow_html=True)
                            st.dataframe(res['success_list'], use_container_width=True, height=250, hide_index=True)
                        with c_fail:
                            st.markdown(f"❌ <span style='color:red;font-size:14px'>매핑 실패 (총 {len(res['fail_list'])}개 유형)</span>", unsafe_allow_html=True)
                            st.dataframe(res['fail_list'], use_container_width=True, height=250, hide_index=True)
                        st.divider()

                st.subheader("📝 최종 전처리 SQL (BigQuery)")
                full_sql = "SELECT\n  *,\n" + ",\n".join(sql_parts) + "\nFROM `데이터셋.테이블`"
                st.code(full_sql, language="sql")

# ==========================================
# 🔵 메뉴 2: 마스터 테이블 관리 (업로드 모드 추가)
# ==========================================
elif menu == "마스터 테이블 관리":
    st.title("🗂️ 마스터 테이블 등록 및 관리")
    st.info("DB에 마스터 테이블을 새로 만들거나 데이터를 추가/갱신할 수 있습니다.")

    # [수정] 테이블 명칭과 업로드 방식을 한 줄에 배치
    col_t_name, col_t_mode = st.columns([2, 1])
    
    with col_t_name:
        new_table_name = st.text_input("마스터 테이블 영문 이름", placeholder="예: channel_info_m").strip().lower()
    
    with col_t_mode:
        upload_mode_label = st.radio(
            "업로드 방식 선택",
            ["Replace", "Delete & Insert", "Append"],
            index=0,
            help="재생성: 스키마가 바뀔 때 / 비우고 삽입: 전체 데이터 교체 / 추가: 누적 데이터"
        )
        # UI 라벨을 DB 커넥터용 코드로 매핑
        mode_map = {
            "Replace": "replace",
            "Delete & Insert": "delete_insert",
            "Append": "append"
        }
        selected_mode = mode_map[upload_mode_label]

    if 'master_df' not in st.session_state:
        st.session_state['master_df'] = None

    tab1, tab2 = st.tabs(["📋 직접 붙여넣기 (권장)", "📁 파일 업로드"])

    with tab1:
        st.markdown("데이터와 **컬럼 헤더**를 붙여넣기 하세요.")
        pasted_master = st.text_area("데이터 영역:", height=200, placeholder="여기에 마스터 데이터를 붙여넣으세요...")
        if st.button("마스터 데이터 읽기", key="btn_master_paste"):
            if pasted_master.strip():
                try:
                    st.session_state['master_df'] = pd.read_csv(io.StringIO(pasted_master), sep='\t')
                    st.success("데이터를 성공적으로 읽었습니다!")
                except Exception as e:
                    st.error(f"데이터를 읽는 중 오류가 발생했습니다: {e}")

    with tab2:
        uploaded_master = st.file_uploader("마스터 엑셀/CSV 업로드", type=['xlsx', 'csv'], key="uf_master")
        if st.button("마스터 파일 읽기", key="btn_master_upload"):
            if uploaded_master:
                try:
                    if uploaded_master.name.endswith('.csv'):
                        st.session_state['master_df'] = pd.read_csv(uploaded_master)
                    else:
                        st.session_state['master_df'] = pd.read_excel(uploaded_master)
                    st.success("파일을 성공적으로 읽었습니다!")
                except Exception as e:
                    st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    master_df = st.session_state['master_df']

    if master_df is not None and not master_df.empty:
        st.subheader("👀 등록할 마스터 데이터 미리보기")
        st.dataframe(master_df.head(5), use_container_width=True)
        st.write(f"건수: {len(master_df)}건 / 컬럼: {len(master_df.columns)}개 / 방식: **{upload_mode_label}**")

        if st.button(f"🚀 {upload_mode_label} 방식으로 최종 등록하기", type="primary"):
            if not new_table_name:
                st.warning("테이블 이름을 먼저 입력해 주세요!")
            elif not re.match("^[a-z0-9_]+$", new_table_name):
                st.warning("이름은 영문 소문자, 숫자, 언더바(_)만 가능합니다.")
            else:
                with st.spinner(f"데이터를 {upload_mode_label} 중..."):
                    # [수정] db.upload_master_table 호출 시 selected_mode 인자 전달
                    success, message = db.upload_master_table(master_df, new_table_name, selected_mode)
                    
                    if success:
                        st.success(message)
                        st.balloons()
                        st.session_state['master_df'] = None
                    else:
                        # 테이블이 없는데 append/truncate 하려고 할 때의 가이드 추가
                        if "does not exist" in str(message).lower():
                            st.error("해당 테이블이 존재하지 않습니다. 먼저 '테이블 재생성'으로 최초 등록을 해주세요.")
                        else:
                            st.error(message)