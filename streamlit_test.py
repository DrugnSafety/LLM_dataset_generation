import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
import json
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------
# (0) Streamlit 페이지 설정
# ---------------------------------------------------------------------
st.set_page_config(page_title="Patient Data Processing", layout="wide")

# ---------------------------------------------------------------------
# (1) Google Sheets, DB 연동 설정
# ---------------------------------------------------------------------
# json_file_name = "crfspreadsheet-cb06c09c617b.json"  # 실제 파일 경로
# scopes = [
#     "https://spreadsheets.google.com/feeds",
#     "https://www.googleapis.com/auth/drive",
# ]
# credentials = Credentials.from_service_account_file(json_file_name, scopes=scopes)
# gc = gspread.authorize(credentials)

# 새 코드: 파일 업로드를 통한 인증 처리
st.sidebar.header("Google API 인증")
uploaded_file = st.sidebar.file_uploader("Google 서비스 계정 JSON 파일 업로드", type=["json"])

def authenticate_google_api(json_content):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp_file:
        temp_file.write(json_content)
        temp_file_path = temp_file.name
    try:
        credentials = Credentials.from_service_account_file(temp_file_path, scopes=scopes)
        gc = gspread.authorize(credentials)
        return gc
    finally:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

if "gc" not in st.session_state:
    st.session_state.gc = None

if uploaded_file is not None:
    try:
        json_content = uploaded_file.getvalue()
        st.session_state.gc = authenticate_google_api(json_content)
        st.sidebar.success("Google API 인증 성공!")
    except Exception as e:
        st.sidebar.error(f"인증 실패: {e}")
        st.session_state.gc = None

DB_PARAMS = {
    "host": "222.116.163.76",
    "user": "postgres",
    "password": "postgres",
    "dbname": "HIRA_DB",
    "port": "5432",
}

# OHDSI DB 파라미터
DB_PARAMS_OHDSI = {
    "host": "222.116.163.76",
    "user": "ohdsi_user",
    "password": "postgre",
    "dbname": "ohdsi_db",
    "port": "5432",
}


class DatabaseManager:
    def __init__(self, db_params):
        self.db_params = db_params
        self.engine = None

    def __enter__(self):
        connection_string = (
            f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
            f"@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['dbname']}"
        )
        self.engine = create_engine(connection_string)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.engine:
            self.engine.dispose()

    def execute_query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            columns = result.keys()
            df = pd.DataFrame(result.fetchall(), columns=columns)
            return df


def get_atc_codes_for_medication_codes(medication_codes):
    atc_map = {}
    with DatabaseManager(DB_PARAMS) as db_manager:
        for code in medication_codes:
            code_str = str(code).zfill(9)
            query = f"""
                SELECT DISTINCT atc코드
                FROM hira_02.hiradb_202307
                WHERE CAST("제품코드(개정후)" AS VARCHAR(9)) = '{code_str}'
            """
            atc_info = db_manager.execute_query(query)
            if not atc_info.empty:
                atc_map[code] = atc_info["atc코드"].tolist()
            else:
                atc_map[code] = [None]
    return atc_map


def calculate_age_from_birthdate(birthdate_str: str) -> int:
    if not birthdate_str:
        return None

    try:
        bd_parsed = datetime.strptime(birthdate_str, "%Y-%m-%d")
    except ValueError:
        try:
            bd_parsed = datetime.strptime(birthdate_str, "%Y%m%d")
        except ValueError:
            return None

    today = date.today()
    age = today.year - bd_parsed.year
    if (today.month, today.day) < (bd_parsed.month, bd_parsed.day):
        age -= 1
    return max(age, 0)


# ---------------------------------------------------------------------
# (2) 세션 스테이트 초기화
# ---------------------------------------------------------------------
if "df_patient_demographic" not in st.session_state:
    st.session_state.df_patient_demographic = pd.DataFrame()

if "df_mydrug" not in st.session_state:
    st.session_state.df_mydrug = pd.DataFrame()

if "df_mydrug_current" not in st.session_state:
    st.session_state.df_mydrug_current = pd.DataFrame()

if "df_mydrug_new" not in st.session_state:
    st.session_state.df_mydrug_new = pd.DataFrame()

if "df_mydiagnosis" not in st.session_state:
    st.session_state.df_mydiagnosis = pd.DataFrame()

if "df_myadrs" not in st.session_state:
    st.session_state.df_myadrs = pd.DataFrame()

if "chosen_adr" not in st.session_state:
    st.session_state.chosen_adr = pd.DataFrame()

if "final_output_json" not in st.session_state:
    st.session_state.final_output_json = {}


def load_worksheet_as_df(worksheet):
    rows = worksheet.get_all_values()
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    return df


def process_medication_data(df_mydrug: pd.DataFrame) -> pd.DataFrame:
    if df_mydrug.empty:
        return df_mydrug

    df_mydrug["약품코드"] = df_mydrug["약품코드"].astype(str)
    df_mydrug["조제일자"] = pd.to_datetime(
        df_mydrug["조제일자"], format="%Y%m%d", errors="coerce"
    )
    df_mydrug["일수"] = pd.to_numeric(df_mydrug["일수"], errors="coerce")

    df_mydrug["earliest_date"] = df_mydrug.groupby("병원등록번호")["조제일자"].transform("min")
    df_mydrug["latest_date"] = df_mydrug.groupby("병원등록번호")["조제일자"].transform("max")
    df_mydrug["duration"] = (
        df_mydrug["latest_date"] - df_mydrug["earliest_date"]
    ).dt.days.fillna(0)

    medication_codes = df_mydrug["약품코드"].unique()
    atc_map = get_atc_codes_for_medication_codes(medication_codes)
    df_mydrug["atc_code"] = df_mydrug["약품코드"].apply(lambda c: atc_map.get(c, [None]))

    df_mydrug["atc_code_str"] = df_mydrug["atc_code"].apply(
        lambda x: (
            ", ".join(str(item) for item in x if item)
            if isinstance(x, list)
            else str(x)
        )
    )

    return df_mydrug


st.sidebar.header("구글 스프레드시트 URL")
input_spreadsheet_url = st.sidebar.text_input(
    "구글 스프레드시트(입력) URL",
    value="https://docs.google.com/spreadsheets/d/1368rJ5YYmABPHDdeDXoFJOiJRe7QACTM8fnCLKYm3zw/edit?gid=1332425145#gid=1332425145",
)
output_spreadsheet_url = st.sidebar.text_input(
    "구글 스프레드시트(출력) URL",
    value="https://docs.google.com/spreadsheets/d/1LtuOAXq7-KBX5t8IoGenmizMt69Ra9YlYlN9VLZ_6go/edit?usp=sharing",
)

st.sidebar.subheader("환자등록번호 입력")
patient_id = st.sidebar.text_input("환자등록번호", value="")


def reset_session():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.experimental_set_query_params()
    st.experimental_rerun()


if st.sidebar.button("Reset"):
    reset_session()


# ---------------------------------------------------------------------
# [UPDATED PART] : DB 조회 ( SNOMED→ICD10 ) + person_mapping
# ---------------------------------------------------------------------
def load_diagnosis_data_from_db(pid: str) -> pd.DataFrame:
    """
    (1) person_mapping_key.xlsx 에서 hospital_id -> person_id 찾기
    (2) SNOMED→ICD10 매핑 (OMOP CDM condition_occurrence) 조회
    (3) 결과 DataFrame 반환
    """
    import pandas as pd

    # 1) person_mapping_key.xlsx 불러오기
    try:
        df_map = pd.read_excel(
            "person_mapping_key.xlsx",
            dtype={"hospital_id": str, "person_id": int},
        )
    except Exception as e:
        raise Exception(f"person_mapping_key.xlsx 로드 실패: {e}")

    # hospital_id → 8자리 zero-fill
    df_map["hospital_id"] = df_map["hospital_id"].fillna("").str.zfill(8)

    # 2) pid(=병원등록번호)와 매칭
    row_mapping = df_map[df_map["hospital_id"] == pid]
    if row_mapping.empty:
        raise Exception(f"person_mapping에서 hospital_id={pid}를 찾을 수 없음.")

    real_person_id = row_mapping.iloc[0]["person_id"]
    if pd.isnull(real_person_id):
        raise Exception(f"person_id가 존재하지 않음 (hospital_id={pid})")
    real_person_id = int(real_person_id)

    # 3) OMOP DB에서 SNOMED→ICD10 매핑된 condition_occurrence 조회
    query = f"""
        SELECT DISTINCT
               co.person_id,
               co.condition_start_date,
               co.condition_source_value AS source_value_original,
               icd.concept_name         AS icd10_concept_name,
               icd.concept_code         AS icd10_code
          FROM cbnuh_omop_cdm.condition_occurrence AS co
          JOIN vocab_202210.concept AS snomed
            ON co.condition_concept_id = snomed.concept_id
          JOIN vocab_202210.concept_relationship AS cr
            ON snomed.concept_id = cr.concept_id_1
          JOIN vocab_202210.concept AS icd
            ON cr.concept_id_2 = icd.concept_id
         WHERE co.person_id = {real_person_id}
           AND snomed.vocabulary_id   = 'SNOMED'
           AND cr.relationship_id     = 'Mapped from'
           AND icd.vocabulary_id      = 'ICD10'
           AND snomed.invalid_reason  IS NULL
           AND icd.invalid_reason     IS NULL
           AND cr.invalid_reason      IS NULL
           AND co.condition_source_value = icd.concept_code
    """

    with DatabaseManager(DB_PARAMS_OHDSI) as db_manager:
        df_db = db_manager.execute_query(query)

    if df_db is None or df_db.empty:
        # DB는 조회했으나 결과가 없는 경우
        return pd.DataFrame()

    # 병원등록번호(=pid) 컬럼 추가
    df_db["병원등록번호"] = pid

    # DB 컬럼을 기존 코드와 호환되도록 rename
    #  - icd10_code -> condition_source_value
    #  - icd10_concept_name -> concept_name
    df_db.rename(
        columns={
            "icd10_code": "condition_source_value",
            "icd10_concept_name": "concept_name",
        },
        inplace=True,
    )

    # 필요하다면 source_value_original은 drop
    df_db.drop(columns=["source_value_original"], inplace=True, errors="ignore")

    # 최종 컬럼 정리
    # ex) ["condition_start_date", "condition_source_value", "concept_name", "person_id", "병원등록번호"]
    return df_db


def load_diagnosis_data(pid: str, doc) -> pd.DataFrame:
    """
    (A) OMOP DB에서 먼저 SNOMED→ICD10 매핑 진단 조회
    (B) DB empty or 에러 시 구글 시트에서 진단명 불러오기
    """
    df_diagnosis = pd.DataFrame()

    # (A) DB에서 조회
    try:
        df_from_db = load_diagnosis_data_from_db(pid)
        if df_from_db is not None and not df_from_db.empty:
            # 중복 제거
            df_from_db.drop_duplicates(
                subset=["condition_source_value", "concept_name"],
                inplace=True,
            )
            df_diagnosis = df_from_db.copy()
        else:
            raise Exception("Empty from DB.")
    except Exception as ex_db:
        st.warning(
            f"OMOP DB 진단정보 없음. 구글 시트에서 로드 시도.\nDB 조회 실패(또는 empty): {ex_db}"
        )
        # (B) 구글 시트 fallback
        try:
            ws_diagnosis = doc.worksheet("진단명")
            df_diag_raw = load_worksheet_as_df(ws_diagnosis)
            if "병원등록번호" not in df_diag_raw.columns:
                if "hospital_id" in df_diag_raw.columns:
                    df_diag_raw.rename(
                        columns={"hospital_id": "병원등록번호"}, inplace=True
                    )
                else:
                    st.warning(
                        "'진단명' 시트에 '병원등록번호' 또는 'hospital_id' 컬럼이 없어 진단 데이터를 불러올 수 없습니다."
                    )
                    df_diag_raw = pd.DataFrame()

            # **현재 pid와 일치하는 행만 필터링** (중요!)
            df_diag_raw["병원등록번호"] = (
                df_diag_raw["병원등록번호"].astype(str).str.zfill(8)
            )
            df_diag_raw = df_diag_raw[df_diag_raw["병원등록번호"] == pid]

            # 중복 제거
            df_diag_raw.drop_duplicates(
                subset=["condition_source_value"], inplace=True
            )

            df_diagnosis = df_diag_raw.copy()

        except Exception as ex_sheet:
            st.error(f"구글 시트 진단 로드 실패: {ex_sheet}")
            df_diagnosis = pd.DataFrame()

    # 최종적으로 얻은 df_diagnosis
    return df_diagnosis


if st.sidebar.button("Load Data"):
    if st.session_state.gc is None:
        st.error("Google API 인증 파일을 업로드해 주세요.")
        st.stop()
    try:
        doc = st.session_state.gc.open_by_url(input_spreadsheet_url)
        pid = str(patient_id).zfill(8)

        # (A) 환자 demographics
        ws_demo = doc.worksheet("환자_list")
        df_demo = load_worksheet_as_df(ws_demo)

        if "병원등록번호" not in df_demo.columns:
            if "hospital_id" in df_demo.columns:
                df_demo.rename(columns={"hospital_id": "병원등록번호"}, inplace=True)
            else:
                raise ValueError(
                    "환자_list 시트에 '병원등록번호' 또는 'hospital_id' 컬럼이 없습니다."
                )

        df_demo["병원등록번호"] = df_demo["병원등록번호"].apply(
            lambda x: str(x).zfill(8) if pd.notnull(x) else ""
        )
        df_demo_filtered = df_demo[df_demo["병원등록번호"] == pid]
        st.session_state.df_patient_demographic = df_demo_filtered

        # (B) 약물 시트
        ws_mydrug = doc.worksheet("내가먹는약_조회")
        df_mydrug_raw = load_worksheet_as_df(ws_mydrug)

        if "병원등록번호" not in df_mydrug_raw.columns:
            if "hospital_id" in df_mydrug_raw.columns:
                df_mydrug_raw.rename(
                    columns={"hospital_id": "병원등록번호"}, inplace=True
                )
            else:
                st.warning(
                    "'내가먹는약_조회' 시트에 '병원등록번호' 또는 'hospital_id' 컬럼이 없어 약물 데이터를 불러올 수 없습니다."
                )
                st.session_state.df_mydrug = pd.DataFrame()
                df_mydrug_filtered = pd.DataFrame()
        else:
            df_mydrug_raw["병원등록번호"] = df_mydrug_raw["병원등록번호"].apply(
                lambda x: str(x).zfill(8)
            )
            df_mydrug_filtered = df_mydrug_raw[df_mydrug_raw["병원등록번호"] == pid]

        if not df_mydrug_filtered.empty:
            st.session_state.df_mydrug = process_medication_data(df_mydrug_filtered)
        else:
            st.warning("해당 환자에 대한 약물 데이터가 없습니다.")
            st.session_state.df_mydrug = pd.DataFrame()

        # (C) 진단명 로드 (DB → fallback: 구글시트)
        df_diagnosis_filtered = load_diagnosis_data(pid, doc)
        if not df_diagnosis_filtered.empty:
            st.info("진단 데이터를 불러왔습니다.")
        else:
            st.warning("해당 환자 진단명이 (DB+시트) 모두 없음.")
        st.session_state.df_mydiagnosis = df_diagnosis_filtered

        # (D) ADR 시트
        ws_adrs = doc.worksheet("ADR_list")
        df_adrs_raw = load_worksheet_as_df(ws_adrs)

        if "병원등록번호" not in df_adrs_raw.columns:
            if "hospital_id" in df_adrs_raw.columns:
                df_adrs_raw.rename(
                    columns={"hospital_id": "병원등록번호"}, inplace=True
                )
            else:
                st.warning(
                    "'ADR_list' 시트에 '병원등록번호' 또는 'hospital_id' 컬럼이 없어 ADR 데이터를 불러올 수 없습니다."
                )
                st.session_state.df_myadrs = pd.DataFrame()
                df_adrs_filtered = pd.DataFrame()
        else:
            df_adrs_raw["병원등록번호"] = (
                df_adrs_raw["병원등록번호"].astype(str).str.zfill(8)
            )
            df_adrs_filtered = df_adrs_raw[df_adrs_raw["병원등록번호"] == pid]

        if not df_adrs_filtered.empty:
            st.session_state.df_myadrs = df_adrs_filtered.copy()
        else:
            st.warning("이 환자에 대한 ADR 데이터가 없습니다.")
            st.session_state.df_myadrs = pd.DataFrame()

        st.sidebar.success("데이터 로드 완료.")

    except Exception as ex:
        st.sidebar.error(f"Error loading data: {ex}")


# ---------------------------------------------------------------------
# (5) Sidebar: 환자 Demographics 표시/수정
# ---------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("Patient Demographics")

if not st.session_state.df_patient_demographic.empty:
    pat_info = st.session_state.df_patient_demographic.iloc[0]
    full_name = pat_info.get("성명", "")
    initial_name = get_initial(full_name)  # 초성 변환 함수 호출
    name = st.sidebar.text_input("성명 (초성)", value=initial_name)
    research_id = st.sidebar.text_input("연구등록번호", value=pat_info.get("연구등록번호", ""))
    hospital_id = st.sidebar.text_input("병원등록번호", value=pat_info.get("병원등록번호", ""))
    birth_date = st.sidebar.text_input("생년월일_new", value=pat_info.get("생년월일_new", ""))
    raw_gender = pat_info.get("성별", "M")
    if raw_gender not in ["M", "F"]:
        raw_gender = "M"
    gender = st.sidebar.selectbox("성별", ["M", "F"], index=["M", "F"].index(raw_gender))

    if st.sidebar.button("Update Patient Info"):
        # 업데이트 시 입력된 초성으로 변경되지만, 원래 full name은 별도 조정 필요
        st.session_state.df_patient_demographic.at[0, "성명"] = name  
        st.session_state.df_patient_demographic.at[0, "연구등록번호"] = research_id
        st.session_state.df_patient_demographic.at[0, "병원등록번호"] = hospital_id
        st.session_state.df_patient_demographic.at[0, "생년월일_new"] = birth_date
        st.session_state.df_patient_demographic.at[0, "성별"] = gender
        st.sidebar.success("환자 정보가 업데이트되었습니다.")
else:
    st.sidebar.info("데이터 로드 후 환자정보가 표시됩니다.")


# ---------------------------------------------------------------------
# (6) Drug Selection
# ---------------------------------------------------------------------
def show_medication_data():
    st.subheader("Medication Data (All)")
    if st.session_state.df_mydrug.empty:
        st.info("No medication data.")
    else:
        st.dataframe(st.session_state.df_mydrug)


def show_current_medication():
    st.subheader("Current Medication")
    df = st.session_state.df_mydrug.copy()
    if df.empty:
        st.warning("약물 데이터가 없습니다.")
        return

    df["약품코드"] = df["약품코드"].astype(str)
    df["med_days"] = df.groupby("약품코드")["일수"].transform("sum")

    total_duration = df["duration"].iloc[0] if len(df) > 0 else 0
    one_third = total_duration / 3.0
    st.write(f"총 duration: {total_duration}, 1/3 = {one_third:.1f}")

    df_above = df[df["med_days"] >= one_third].drop_duplicates(subset=["약품코드"])
    df_below = df[df["med_days"] < one_third].drop_duplicates(subset=["약품코드"])

    st.markdown("#### [1/3 이상 처방 약물]")
    if df_above.empty:
        df_above_disp = pd.DataFrame(columns=["약품코드", "약품명", "med_days"])
        df_above_disp["select"] = False
    else:
        df_above["select"] = True  # 기본 True
        df_above_disp = df_above[["select", "약품코드", "약품명", "med_days"]]

    edited_above = st.data_editor(
        df_above_disp, key="current_above", use_container_width=True
    )

    st.markdown("#### [1/3 미만 처방 약물]")
    if df_below.empty:
        df_below_disp = pd.DataFrame(
            columns=["select", "약품코드", "약품명", "med_days", "atc_code_str"]
        )
    else:
        df_below["select"] = False
        needed_cols = ["select", "약품코드", "약품명", "med_days", "atc_code_str"]
        for c in needed_cols:
            if c not in df_below.columns:
                df_below[c] = None
        df_below_disp = df_below[needed_cols]

    edited_below = st.data_editor(
        df_below_disp, key="current_below", use_container_width=True
    )

    if st.button("Confirm Current Medication"):
        chosen_above = (
            edited_above[edited_above["select"] == True]
            if not edited_above.empty
            else pd.DataFrame()
        )
        chosen_below = (
            edited_below[edited_below["select"] == True]
            if not edited_below.empty
            else pd.DataFrame()
        )

        final_codes = []
        if not chosen_above.empty:
            final_codes += chosen_above["약품코드"].tolist()
        if not chosen_below.empty:
            final_codes += chosen_below["약품코드"].tolist()

        final_codes = list(set(final_codes)) 
        final_df = df[df["약품코드"].isin(final_codes)].drop_duplicates(subset=["약품코드"])

        keep_cols = [
            "조제일자",
            "약품코드",
            "약품명",
            "성분명",
            "atc_code_str",
            "med_days",
        ]
        exist_cols = [c for c in keep_cols if c in final_df.columns]
        st.session_state.df_mydrug_current = final_df[exist_cols].reset_index(drop=True)

        st.success("Current Medication 확정되었습니다.")
        st.dataframe(st.session_state.df_mydrug_current)


def show_newly_prescription():
    st.subheader("Newly Prescription")
    df = st.session_state.df_mydrug.copy()
    if df.empty:
        st.warning("약물 데이터가 없습니다.")
        return

    df["조제일자"] = pd.to_datetime(df["조제일자"], errors="coerce")
    unique_dates = sorted(df["조제일자"].dropna().unique())

    selected_date = st.selectbox(
        "날짜 선택",
        [None] + list(unique_dates),
        format_func=lambda x: x.strftime("%Y-%m-%d") if x else "날짜를 선택",
    )

    if not selected_date:
        st.info("날짜를 선택하면 해당 날짜의 처방약물을 표시합니다.")
        return

    subset = df[df["조제일자"] == selected_date].copy()
    if subset.empty:
        st.info("해당 날짜의 약물이 없습니다.")
        return

    if "select" not in subset.columns:
        subset["select"] = False

    needed_cols = [
        "select",
        "조제일자",
        "약품코드",
        "약품명",
        "성분명",
        "atc_code_str",
        "투약량",
        "함량",
        "횟수",
        "일수",
    ]
    for c in needed_cols:
        if c not in subset.columns:
            subset[c] = None

    subset_display = subset[needed_cols].reset_index(drop=True)
    edited_subset = st.data_editor(
        subset_display, key="new_prescript_editor", use_container_width=True
    )

    if st.button("Confirm Newly Prescription"):
        chosen = edited_subset[edited_subset["select"] == True]
        if chosen.empty:
            st.warning("아무것도 선택되지 않았습니다.")
        else:
            st.session_state.df_mydrug_new = chosen.drop_duplicates(
                subset=["약품코드"]
            ).reset_index(drop=True)
            st.success(f"{selected_date.strftime('%Y-%m-%d')} 날짜 처방약물 확정됨.")
            st.dataframe(st.session_state.df_mydrug_new)


# ---------------------------------------------------------------------
# (7) '진단명 추가' 알고리즘(예시)
# ---------------------------------------------------------------------
comorbidity_map = {
    "hypertension": {
        "atc_prefixes": ["C02", "C03", "C07", "C08", "C09"],
        "icd_info": {
            "condition_source_value": "I10.0",
            "condition_source_concept_id": "Essential (primary) Hypertension",
        },
    },
    "diabetes": {
        "atc_prefixes": ["A10"],
        "icd_info": {
            "condition_source_value": "E10.0",
            "condition_source_concept_id": "Type 2 diabetes mellitus",
        },
    },
    "dyslipidemia": {
        "atc_prefixes": ["C10"],
        "icd_info": {
            "condition_source_value": "E78.5",
            "condition_source_concept_id": "dyslipidemia",
        },
    },
}


def generate_comorbidity_diagnosis(df_current: pd.DataFrame) -> pd.DataFrame:
    if df_current.empty:
        return pd.DataFrame()

    # 이미 session_state.df_mydiagnosis가 있는 경우, 그 ICD10코드를 set()으로 수집
    existing_codes = (
        set(st.session_state.df_mydiagnosis["condition_source_value"].astype(str))
        if not st.session_state.df_mydiagnosis.empty
        else set()
    )

    results = []
    for _, row in df_current.iterrows():
        drugName = row.get("약품명", "")
        atcStr = row.get("atc_code_str", "")
        atc_list = [s.strip() for s in atcStr.split(",") if s.strip()]

        for atc_single in atc_list:
            for disease_name, info in comorbidity_map.items():
                prefixes = info["atc_prefixes"]
                icd_info = info["icd_info"]
                if any(atc_single.startswith(pref) for pref in prefixes):
                    code_val = icd_info["condition_source_value"]
                    if code_val not in existing_codes:
                        results.append(
                            {
                                "select": False,
                                "drugName": drugName,
                                "atcCode": atc_single,
                                # "condition_source_concept_id" 컬럼 대신 concept_name 하나로 통일
                                "condition_source_value": code_val,
                                "concept_name": icd_info["condition_source_concept_id"],
                            }
                        )

    if not results:
        return pd.DataFrame()

    df_result = pd.DataFrame(results).drop_duplicates(
        subset=["condition_source_value"]
    )
    return df_result

def show_diagnosis_selection():
    """
    (1) DB+구글 시트에서 진단 로드 
    (2) '진단명 조회' 탭에서 선택
    (3) '진단명 추가' 탭(Comorbidity)
    """
    st.title("Diagnosis Selection")
    diag_subtabs = st.tabs(["진단명 조회", "진단명 추가"])

    # (A) 진단명 조회
    with diag_subtabs[0]:
        st.subheader("진단명 조회")

        df_diag_raw = st.session_state.df_mydiagnosis.copy()
        if df_diag_raw.empty:
            st.warning("해당 환자에 대한 진단 기록이 없습니다 (DB+시트 모두 조회 실패).")
            return

        # concept_name, condition_source_concept_id가 따로 있을 경우 통합
        # 여기서는 'concept_name'이 최종 진단명, 필요하면 아래 로직을 통해 합치기
        if "concept_name" not in df_diag_raw.columns:
            df_diag_raw["concept_name"] = ""

        # (통합) 만약 'condition_source_concept_id'가 존재한다면,
        # concept_name이 비어있을 경우 해당 값으로 대체하고, 이후 컬럼 제거
        if "condition_source_concept_id" in df_diag_raw.columns:
            for idx, row in df_diag_raw.iterrows():
                if not row["concept_name"]:
                    df_diag_raw.at[idx, "concept_name"] = row["condition_source_concept_id"]
            df_diag_raw.drop(columns=["condition_source_concept_id"], inplace=True)

        # select 컬럼이 없으면 추가
        if "select" not in df_diag_raw.columns:
            df_diag_raw["select"] = False

        # 보여줄 순서 지정
        wanted_cols = ["select", "condition_source_value", "concept_name"]
        other_cols = [c for c in df_diag_raw.columns if c not in wanted_cols]
        df_diag_raw = df_diag_raw[wanted_cols + other_cols]

        st.markdown("**필요한 진단명에 체크 후 아래 Update 버튼을 누르세요**")
        edited_diag = st.data_editor(
            df_diag_raw, key="diag_view_editor", use_container_width=True
        )

        if st.button("Update Diagnosis(조회)"):
            chosen = edited_diag[edited_diag["select"] == True]
            if chosen.empty:
                st.warning("선택된 진단명이 없습니다.")
            else:
                # NaN -> ""
                chosen.fillna("", inplace=True)

                keep_cols = ["condition_source_value", "concept_name"]
                exist_cols = [c for c in keep_cols if c in chosen.columns]
                chosen_clean = chosen[exist_cols].drop_duplicates()

                st.session_state.df_mydiagnosis = chosen_clean.reset_index(drop=True)
                st.success("진단명 조회 Update 완료.")
                st.dataframe(st.session_state.df_mydiagnosis)

    # (B) 진단명 추가
    with diag_subtabs[1]:
        st.subheader("진단명 추가 (Comorbidity Algorithm)")

        if st.session_state.df_mydrug_current.empty:
            st.info("Current Medication이 없어서 Comorbidity 추론을 할 수 없습니다.")
        else:
            df_comorb = generate_comorbidity_diagnosis(
                st.session_state.df_mydrug_current
            )
            if df_comorb.empty:
                st.warning("추론된 Comorbidity 진단이 없습니다(이미 존재하거나, 해당 ATC 없음).")
            else:
                st.markdown("**아래 추론된 진단명 체크 후 Update**")

                # select 없으면 추가
                if "select" not in df_comorb.columns:
                    df_comorb["select"] = False

                wanted_cols = ["select", "condition_source_value", "concept_name", "drugName", "atcCode"]
                for c in wanted_cols:
                    if c not in df_comorb.columns:
                        df_comorb[c] = ""

                other_cols = [c for c in df_comorb.columns if c not in wanted_cols]
                df_comorb = df_comorb[wanted_cols + other_cols]

                edited_newdiag = st.data_editor(
                    df_comorb, key="comorb_add_editor", use_container_width=True
                )

                if st.button("Update (추가 진단)"):
                    chosen_new = edited_newdiag[edited_newdiag["select"] == True]
                    if chosen_new.empty:
                        st.warning("선택된 진단명이 없습니다.")
                    else:
                        chosen_new.fillna("", inplace=True)
                        keep_cols = ["condition_source_value", "concept_name"]
                        exist_cols = [c for c in keep_cols if c in chosen_new.columns]
                        chosen_clean = chosen_new[exist_cols].drop_duplicates()

                        if st.session_state.df_mydiagnosis.empty:
                            st.session_state.df_mydiagnosis = chosen_clean.copy()
                        else:
                            st.session_state.df_mydiagnosis = pd.concat(
                                [st.session_state.df_mydiagnosis, chosen_clean],
                                ignore_index=True,
                            ).drop_duplicates(subset=["condition_source_value"])

                        st.success("추가된 진단명 리스트 업데이트 완료.")
                        st.dataframe(st.session_state.df_mydiagnosis)


# ---------------------------------------------------------------------
# (9) ADR Selection
# ---------------------------------------------------------------------
def show_adr_selection():
    st.title("ADR Selection")
    df_adr = st.session_state.df_myadrs.copy()
    if df_adr.empty:
        st.info("이 환자의 ADR 데이터가 없습니다.")
        return

    df_adr["combined_adr"] = df_adr.apply(
        lambda row: (
            f"{row.get('ADR_annocation','')} ({row.get('action_plan_for_tolerable_drugs','')})"
            if pd.notnull(row.get("ADR_annocation", ""))
            and pd.notnull(row.get("action_plan_for_tolerable_drugs", ""))
            else (
                row.get("ADR_annocation", "")
                if pd.notnull(row.get("ADR_annocation", ""))
                else "null"
            )
        ),
        axis=1,
    )

    if "select" not in df_adr.columns:
        df_adr["select"] = False

    display_cols = [
        "select",
        "combined_adr",
        "현재상태_통합조정",
        "유형_통합조정",
        "중증도_통합조정",
        "인과성_통합조정",
        "ADR_annocation",
        "action_plan_for_tolerable_drugs",
        "전문가의견_통합조정",
    ]
    for c in display_cols:
        if c not in df_adr.columns:
            df_adr[c] = None
    df_adr = df_adr[display_cols]

    st.markdown("**필요한 ADR에 체크 후 Update 버튼을 누르세요**")
    edited_adr = st.data_editor(df_adr, key="adr_editor", use_container_width=True)

    if st.button("Update ADR"):
        chosen_adr = edited_adr[edited_adr["select"] == True]
        if chosen_adr.empty:
            st.warning("선택된 ADR 항목이 없습니다.")
        else:
            chosen_adr.fillna("", inplace=True)
            st.session_state.chosen_adr = chosen_adr.copy().reset_index(drop=True)
            st.success("ADR Selection Update 완료.")
            st.dataframe(st.session_state.chosen_adr)


# ---------------------------------------------------------------------
# (10) Final JSON Output
# ---------------------------------------------------------------------
def show_final_json():
    st.title("Final JSON Output")

    if not st.session_state.df_patient_demographic.empty:
        pat_row = st.session_state.df_patient_demographic.iloc[0]
        birth_date_str = pat_row.get("생년월일_new", "")
        final_age = calculate_age_from_birthdate(birth_date_str)
        if final_age is None:
            final_age = "Unknown"

        raw_gender = pat_row.get("성별", "N/A")
        if raw_gender == "M":
            final_gender = "Male"
        elif raw_gender == "F":
            final_gender = "Female"
        else:
            final_gender = "Unknown"

        name = pat_row.get("성명", "")
        research_id = pat_row.get("연구등록번호", "")
        patient_num = pat_row.get("병원등록번호", "")
    else:
        final_age = None
        final_gender = "Unknown"
        name = ""
        research_id = ""
        patient_num = ""

    # (A) comorbidities => from df_mydiagnosis
    comorbidities = []
    if not st.session_state.df_mydiagnosis.empty:
        df_diag = st.session_state.df_mydiagnosis.copy().fillna("")
        for _, row in df_diag.iterrows():
            code = str(row.get("condition_source_value", "")).strip()
            dname = str(row.get("concept_name", "")).strip()
            if code:
                comorbidities.append(
                    {
                        "diagnosisType": "ICD10",
                        "diagnosisCode": code,
                        # diagnosisName에 concept_name을 넣어주기
                        "diagnosisName": dname,
                    }
                )

    # (B) currentMedication
    currentMedication = []
    if not st.session_state.df_mydrug_current.empty:
        df_cur = st.session_state.df_mydrug_current.copy().fillna("")
        if "조제일자" in df_cur.columns:
            df_cur["조제일자"] = df_cur["조제일자"].astype(str)

        for _, row in df_cur.iterrows():
            kdCode = str(row.get("약품코드", "")).strip()
            kdName = str(row.get("약품명", "")).strip()
            atcCode = str(row.get("atc_code_str", "")).strip()
            currentMedication.append(
                {"kdCode": kdCode, "kdName": kdName, "atcCode": atcCode}
            )

    # (C) newPrescriptions
    newPrescriptions = []
    if not st.session_state.df_mydrug_new.empty:
        df_newp = st.session_state.df_mydrug_new.copy().fillna("")
        if "조제일자" in df_newp.columns:
            df_newp["조제일자"] = df_newp["조제일자"].astype(str)

        for _, row in df_newp.iterrows():
            kdCode = str(row.get("약품코드", "")).strip()
            kdName = str(row.get("약품명", "")).strip()
            atcCode = str(row.get("atc_code_str", "")).strip()
            newPrescriptions.append(
                {"kdCode": kdCode, "kdName": kdName, "atcCode": atcCode}
            )

    # (D) ADRs => JSON에는 description만
    adrs = []
    if not st.session_state.chosen_adr.empty:
        df_adr_sel = st.session_state.chosen_adr.copy().fillna("")
        for _, row in df_adr_sel.iterrows():
            combined_adr = str(row.get("combined_adr", "")).strip()
            if combined_adr:
                adrs.append({"description": combined_adr})

    # 최종 JSON
    final_output = {
        "age": final_age,
        "gender": final_gender,
        "comorbidities": comorbidities,
        "currentMedication": currentMedication,
        "newPrescriptions": newPrescriptions,
        "adrs": adrs,
    }
    st.session_state.final_output_json = final_output

    st.json(final_output)

    if st.button("output sheet upload"):
        doc_out = st.session_state.gc.open_by_url(output_spreadsheet_url)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # (1) baseline_characteristics
        try:
            sheet_baseline = doc_out.worksheet("baseline_characteristics")
        except:
            sheet_baseline = doc_out.add_worksheet(
                title="baseline_characteristics", rows=100, cols=20
            )

        row_data_baseline = [
            patient_num,
            name,
            research_id,
            final_age,
            final_gender,
            now_str,
        ]
        sheet_baseline.append_row(row_data_baseline, value_input_option="USER_ENTERED")

        # (2) current_medication
        try:
            sheet_current_med = doc_out.worksheet("current_medication")
        except:
            sheet_current_med = doc_out.add_worksheet(
                title="current_medication", rows=100, cols=20
            )

        df_cur = st.session_state.df_mydrug_current.copy()
        if not df_cur.empty:
            df_cur.fillna("", inplace=True)
            if "조제일자" in df_cur.columns:
                df_cur["조제일자"] = df_cur["조제일자"].astype(str)

            df_cur["병원등록번호"] = patient_num
            col_order = [
                "병원등록번호",
                "조제일자",
                "약품코드",
                "약품명",
                "성분명",
                "atc_code_str",
                "med_days",
            ]
            for col in col_order:
                if col not in df_cur.columns:
                    df_cur[col] = ""
            df_cur = df_cur[col_order]

            for _, row_data in df_cur.iterrows():
                sheet_current_med.append_row(
                    row_data.tolist(), value_input_option="USER_ENTERED"
                )

        # (3) newly_prescription
        try:
            sheet_new_pres = doc_out.worksheet("newly_prescription")
        except:
            sheet_new_pres = doc_out.add_worksheet(
                title="newly_prescription", rows=100, cols=20
            )

        df_newp = st.session_state.df_mydrug_new.copy()
        if not df_newp.empty:
            df_newp.fillna("", inplace=True)
            if "조제일자" in df_newp.columns:
                df_newp["조제일자"] = df_newp["조제일자"].astype(str)

            df_newp["병원등록번호"] = patient_num
            col_order_new = [
                "병원등록번호",
                "조제일자",
                "약품코드",
                "약품명",
                "성분명",
                "atc_code_str",
                "투약량",
                "함량",
                "횟수",
                "일수",
            ]
            for col in col_order_new:
                if col not in df_newp.columns:
                    df_newp[col] = ""
            df_newp = df_newp[col_order_new]

            for _, row_data in df_newp.iterrows():
                sheet_new_pres.append_row(
                    row_data.tolist(), value_input_option="USER_ENTERED"
                )

        # (4) diagnosis
        try:
            sheet_diagnosis = doc_out.worksheet("diagnosis")
        except:
            sheet_diagnosis = doc_out.add_worksheet(
                title="diagnosis", rows=100, cols=20
            )

        df_diag = st.session_state.df_mydiagnosis.copy()
        if not df_diag.empty:
            df_diag.fillna("", inplace=True)
            df_diag["병원등록번호"] = patient_num
            # concept_name 포함해서 저장
            col_order_diag = [
                "병원등록번호",
                "condition_source_value",
                "concept_name",
            ]
            for col in col_order_diag:
                if col not in df_diag.columns:
                    df_diag[col] = ""
            df_diag = df_diag[col_order_diag]

            for _, row_data in df_diag.iterrows():
                sheet_diagnosis.append_row(
                    row_data.tolist(), value_input_option="USER_ENTERED"
                )

        # (5) adr_selected
        try:
            sheet_adr = doc_out.worksheet("adr_selected")
        except:
            sheet_adr = doc_out.add_worksheet(title="adr_selected", rows=100, cols=30)

        df_adr_sel = st.session_state.chosen_adr.copy()
        if not df_adr_sel.empty:
            df_adr_sel.fillna("", inplace=True)
            df_adr_sel["병원등록번호"] = patient_num
            col_order_adr = [
                "병원등록번호",
                "select",
                "combined_adr",
                "현재상태_통합조정",
                "유형_통합조정",
                "중증도_통합조정",
                "인과성_통합조정",
                "ADR_annocation",
                "action_plan_for_tolerable_drugs",
                "전문가의견_통합조정",
            ]
            for col in col_order_adr:
                if col not in df_adr_sel.columns:
                    df_adr_sel[col] = ""
            df_adr_sel = df_adr_sel[col_order_adr]

            for _, row_data in df_adr_sel.iterrows():
                sheet_adr.append_row(
                    row_data.tolist(), value_input_option="USER_ENTERED"
                )

        # (6) patient_dataset_generation
        try:
            sheet_out = doc_out.worksheet("patient_dataset_generation")
        except:
            sheet_out = doc_out.add_worksheet(
                title="patient_dataset_generation", rows=100, cols=20
            )

        row_data = [
            patient_num,
            name,
            final_age,
            final_gender,
            research_id,
            now_str,
            json.dumps(st.session_state.final_output_json, ensure_ascii=False),
        ]
        sheet_out.append_row(row_data, value_input_option="USER_ENTERED")

        st.success("결과가 모든 시트에 업데이트되었습니다.")


# ---------------------------------------------------------------------
# (11) 메인 탭
# ---------------------------------------------------------------------
tabs_main = st.tabs(
    [
        "Drug Selection",
        "Diagnosis Selection",
        "ADR Selection",
        "Final JSON Output",
    ]
)

with tabs_main[0]:
    st.title("Drug Selection")
    drug_subtabs = st.tabs(
        ["Medication Data", "Current Medication", "Newly Prescription"]
    )
    with drug_subtabs[0]:
        show_medication_data()
    with drug_subtabs[1]:
        show_current_medication()
    with drug_subtabs[2]:
        show_newly_prescription()

with tabs_main[1]:
    show_diagnosis_selection()

with tabs_main[2]:
    show_adr_selection()

with tabs_main[3]:
    show_final_json()
