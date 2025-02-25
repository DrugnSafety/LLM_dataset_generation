# ---------------------------------------------------------------------
# 라이브러리 임포트
# ---------------------------------------------------------------------
import streamlit as st                   # 웹 애플리케이션 구축을 위한 Streamlit (UI 구성 및 인터랙션 처리)
import pandas as pd                      # 데이터 처리 및 분석을 위한 pandas
import gspread                           # 구글 스프레드시트와의 연동을 위한 gspread 라이브러리
from google.oauth2.service_account import Credentials  # 구글 API 인증을 위한 서비스 계정 Credential 생성
from datetime import datetime, date      # 날짜 및 시간 관련 처리를 위한 datetime 모듈
import json                              # JSON 데이터 처리 모듈 (최종 JSON 출력 생성에 사용)
from sqlalchemy import create_engine, text  # SQLAlchemy: 데이터베이스 연결 및 SQL 쿼리 실행을 위한 모듈

# ---------------------------------------------------------------------
# (0) Streamlit 페이지 설정
# ---------------------------------------------------------------------
st.set_page_config(page_title="Patient Data Processing", layout="wide")
# 페이지 제목과 레이아웃 설정 (wide: 화면 전체 사용)

# ---------------------------------------------------------------------
# (1) Google Sheets, DB 연동 설정
# ---------------------------------------------------------------------
# 서비스 계정 JSON 파일 경로 및 필요한 스코프 설정
json_file_name = "starlit-hangar-393004-12301ecb63f5.json"  # 실제 JSON 파일 경로 (구글 API 인증 정보)
scopes = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
# 서비스 계정 인증 정보 로드 후 gspread를 통해 인증
credentials = Credentials.from_service_account_file(json_file_name, scopes=scopes)
gc = gspread.authorize(credentials)

# 기본 DB 연결 파라미터 (HIRA_DB)
DB_PARAMS = {
    "host": "222.116.163.76",      # DB 서버 호스트
    "user": "postgres",            # DB 사용자명
    "password": "postgres",        # DB 비밀번호
    "dbname": "HIRA_DB",           # DB 이름
    "port": "5432",                # DB 포트 (PostgreSQL 기본 포트)
}

# OHDSI DB 연결 파라미터 (OHDSI 관련 데이터 사용)
DB_PARAMS_OHDSI = {
    "host": "222.116.163.76",
    "user": "ohdsi_user",
    "password": "postgre",
    "dbname": "ohdsi_db",
    "port": "5432",
}


# ---------------------------------------------------------------------
# 데이터베이스 연결을 위한 매니저 클래스 정의
# ---------------------------------------------------------------------
class DatabaseManager:
    def __init__(self, db_params):
        """
        생성자: 데이터베이스 접속을 위한 파라미터를 저장합니다.
        """
        self.db_params = db_params
        self.engine = None  # 나중에 SQLAlchemy 엔진 객체를 저장

    def __enter__(self):
        """
        with 문으로 진입 시 DB 연결 문자열을 생성하고 SQLAlchemy 엔진을 초기화합니다.
        """
        connection_string = (
            f"postgresql://{self.db_params['user']}:{self.db_params['password']}"
            f"@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['dbname']}"
        )
        self.engine = create_engine(connection_string)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        with 문 종료 시 엔진을 해제하여 DB 연결을 종료합니다.
        """
        if self.engine:
            self.engine.dispose()

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        SQL 쿼리를 실행하고 결과를 pandas DataFrame으로 반환합니다.
        - 쿼리 실행 후 결과의 컬럼명을 keys로 사용하여 DataFrame 생성.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            columns = result.keys()  # 쿼리 결과 컬럼명 추출
            df = pd.DataFrame(result.fetchall(), columns=columns)
            return df


# ---------------------------------------------------------------------
# (A) 약물 코드로부터 ATC 코드 매핑 함수
# ---------------------------------------------------------------------
def get_atc_codes_for_medication_codes(medication_codes):
    """
    주어진 약물 코드 리스트를 순회하며 DB에서 해당 약물에 대한 ATC 코드를 조회합니다.
    - 각 코드에 대해 9자리 zero-fill 처리를 수행
    - 조회 결과가 있으면 ATC 코드 리스트를, 없으면 [None]을 매핑
    """
    atc_map = {}
    with DatabaseManager(DB_PARAMS) as db_manager:
        for code in medication_codes:
            code_str = str(code).zfill(9)  # 9자리 문자열로 변환 (필요시 0 채움)
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


# ---------------------------------------------------------------------
# (B) 생년월일로부터 나이 계산 함수
# ---------------------------------------------------------------------
def calculate_age_from_birthdate(birthdate_str: str) -> int:
    """
    문자열 형태의 생년월일을 받아 현재 날짜 기준 나이를 계산합니다.
    - 입력 형식은 "%Y-%m-%d" 또는 "%Y%m%d"를 지원합니다.
    - 파싱 실패 시 None 반환.
    - 음수 나이 발생 시 0으로 보정.
    """
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
# (2) Streamlit 세션 상태 초기화
# ---------------------------------------------------------------------
# 각종 데이터프레임 및 변수들을 session_state에 초기화하여, 페이지 재실행 시 값 유지 및 관리
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

# ---------------------------------------------------------------------
# (3) 구글 스프레드시트 데이터 로드를 위한 헬퍼 함수 정의
# ---------------------------------------------------------------------
def load_worksheet_as_df(worksheet):
    """
    구글 스프레드시트의 worksheet 객체로부터 전체 값을 가져와 DataFrame으로 변환합니다.
    - 첫 번째 행은 헤더(컬럼명)로 사용하고, 나머지 행은 데이터로 처리합니다.
    - 데이터가 없으면 빈 DataFrame 반환.
    """
    rows = worksheet.get_all_values()
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    return df


def process_medication_data(df_mydrug: pd.DataFrame) -> pd.DataFrame:
    """
    약물 데이터(DataFrame)를 전처리하는 함수.
    - 약품코드 문자열 변환, 조제일자 형식 변환, 일수(numeric) 변환
    - 병원등록번호별로 조제일자의 최솟값과 최댓값을 구해 duration(일수 차이) 계산
    - 각 약품코드에 대해 ATC 코드를 DB에서 조회 후 매핑
    - ATC 코드를 문자열 형태로 join 하여 추가 컬럼(atc_code_str) 생성
    """
    if df_mydrug.empty:
        return df_mydrug

    # 약품코드 문자열 처리 (숫자코드일 경우에도 문자열로 취급)
    df_mydrug["약품코드"] = df_mydrug["약품코드"].astype(str)
    
    # 조제일자 컬럼을 datetime 형식으로 변환, 형식은 YYYYMMDD, 오류 발생 시 NaT로 처리
    df_mydrug["조제일자"] = pd.to_datetime(
        df_mydrug["조제일자"], format="%Y%m%d", errors="coerce"
    )
    # 일수 컬럼을 숫자형으로 변환, 변환 불가능한 값은 NaN 처리
    df_mydrug["일수"] = pd.to_numeric(df_mydrug["일수"], errors="coerce")

    # 병원등록번호별로 첫 조제일자와 마지막 조제일자 계산
    df_mydrug["earliest_date"] = df_mydrug.groupby("병원등록번호")["조제일자"].transform("min")
    df_mydrug["latest_date"] = df_mydrug.groupby("병원등록번호")["조제일자"].transform("max")
    # 두 날짜 간의 차이를 일(day) 단위로 계산하여 duration 컬럼 생성
    df_mydrug["duration"] = (
        df_mydrug["latest_date"] - df_mydrug["earliest_date"]
    ).dt.days.fillna(0)

    # 고유 약품코드 리스트 추출 후, 각 코드에 대해 ATC 코드 매핑 조회
    medication_codes = df_mydrug["약품코드"].unique()
    atc_map = get_atc_codes_for_medication_codes(medication_codes)
    df_mydrug["atc_code"] = df_mydrug["약품코드"].apply(lambda c: atc_map.get(c, [None]))

    # ATC 코드 리스트를 문자열로 변환하여 별도의 컬럼(atc_code_str)에 저장 (쉼표로 구분)
    df_mydrug["atc_code_str"] = df_mydrug["atc_code"].apply(
        lambda x: (
            ", ".join(str(item) for item in x if item)
            if isinstance(x, list)
            else str(x)
        )
    )

    return df_mydrug


# ---------------------------------------------------------------------
# (4) 사이드바: 구글 스프레드시트 URL 및 환자등록번호 입력 UI
# ---------------------------------------------------------------------
st.sidebar.header("구글 스프레드시트 URL")
# 입력용 및 출력용 구글 스프레드시트 URL을 사이드바에 텍스트 입력창으로 제공
input_spreadsheet_url = st.sidebar.text_input(
    "구글 스프레드시트(입력) URL",
    value="https://docs.google.com/spreadsheets/d/1368rJ5YYmABPHDdeDXoFJOiJRe7QACTM8fnCLKYm3zw/edit?gid=1332425145#gid=1332425145",
)
output_spreadsheet_url = st.sidebar.text_input(
    "구글 스프레드시트(출력) URL",
    value="https://docs.google.com/spreadsheets/d/1LtuOAXq7-KBX5t8IoGenmizMt69Ra9YlYlN9VLZ_6go/edit?usp=sharing",
)

st.sidebar.subheader("환자등록번호 입력")
# 환자 등록번호 입력: 이후 데이터 필터링 및 DB 조회에 사용됨
patient_id = st.sidebar.text_input("환자등록번호", value="")

# ---------------------------------------------------------------------
# (5) 세션 리셋 함수
# ---------------------------------------------------------------------
def reset_session():
    """
    Streamlit의 세션 상태를 초기화하고, 쿼리 파라미터를 리셋 후 페이지 재실행.
    - 개발 및 디버깅 시 상태값 초기화 용도로 활용.
    """
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.experimental_set_query_params()
    st.experimental_rerun()

if st.sidebar.button("Reset"):
    reset_session()


# ---------------------------------------------------------------------
# (6) [UPDATED PART] : DB 조회 ( SNOMED→ICD10 ) + person_mapping
# ---------------------------------------------------------------------
def load_diagnosis_data_from_db(pid: str) -> pd.DataFrame:
    """
    (1) person_mapping_key.xlsx 파일에서 hospital_id에 해당하는 person_id를 찾음
    (2) OMOP CDM의 condition_occurrence 테이블에서 SNOMED 코드와 매핑된 ICD10 정보를 조회
    (3) 조회 결과를 DataFrame으로 반환
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

    # hospital_id를 8자리로 zero-fill 처리
    df_map["hospital_id"] = df_map["hospital_id"].fillna("").str.zfill(8)

    # 2) 입력받은 pid(병원등록번호)와 매핑 파일의 hospital_id 비교
    row_mapping = df_map[df_map["hospital_id"] == pid]
    if row_mapping.empty:
        raise Exception(f"person_mapping에서 hospital_id={pid}를 찾을 수 없음.")

    real_person_id = row_mapping.iloc[0]["person_id"]
    if pd.isnull(real_person_id):
        raise Exception(f"person_id가 존재하지 않음 (hospital_id={pid})")
    real_person_id = int(real_person_id)

    # 3) OMOP DB에서 SNOMED와 ICD10 매핑된 진단 정보 조회 (condition_occurrence 테이블 사용)
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
        # DB는 조회했으나 결과가 없는 경우 빈 DataFrame 반환
        return pd.DataFrame()

    # 병원등록번호(=pid) 컬럼 추가
    df_db["병원등록번호"] = pid

    # DB 컬럼을 기존 코드와 호환되도록 컬럼명 변경
    # - icd10_code -> condition_source_value
    # - icd10_concept_name -> concept_name
    df_db.rename(
        columns={
            "icd10_code": "condition_source_value",
            "icd10_concept_name": "concept_name",
        },
        inplace=True,
    )

    # 필요에 따라 원본 source_value_original 컬럼 삭제
    df_db.drop(columns=["source_value_original"], inplace=True, errors="ignore")

    # 최종 컬럼 정리 및 DataFrame 반환
    return df_db


def load_diagnosis_data(pid: str, doc) -> pd.DataFrame:
    """
    (A) 먼저 OMOP DB에서 SNOMED→ICD10 매핑된 진단 데이터를 조회 시도.
    (B) 만약 DB 조회에 실패하거나 결과가 없으면 구글 시트("진단명" 시트)에서 데이터를 불러옴.
    """
    df_diagnosis = pd.DataFrame()

    # (A) DB에서 조회 시도
    try:
        df_from_db = load_diagnosis_data_from_db(pid)
        if df_from_db is not None and not df_from_db.empty:
            # 중복 제거: condition_source_value와 concept_name 기준
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
        # (B) 구글 시트 fallback: '진단명' 시트에서 데이터 로드
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

            # 현재 pid와 일치하는 행만 필터링 (중요!)
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

    # 최종적으로 얻은 진단 DataFrame 반환
    return df_diagnosis


# ---------------------------------------------------------------------
# (7) Load Data 버튼 클릭 시 구글 스프레드시트 및 DB로부터 데이터 불러오기
# ---------------------------------------------------------------------
if st.sidebar.button("Load Data"):
    try:
        # 입력 스프레드시트 URL로 문서 객체 열기
        doc = gc.open_by_url(input_spreadsheet_url)
        pid = str(patient_id).zfill(8)  # 환자등록번호 8자리로 맞춤

        # (A) 환자 Demographics 로드: "환자_list" 시트 사용
        ws_demo = doc.worksheet("환자_list")
        df_demo = load_worksheet_as_df(ws_demo)

        # '병원등록번호' 컬럼이 없을 경우, 'hospital_id' 컬럼명을 변경하여 사용
        if "병원등록번호" not in df_demo.columns:
            if "hospital_id" in df_demo.columns:
                df_demo.rename(columns={"hospital_id": "병원등록번호"}, inplace=True)
            else:
                raise ValueError(
                    "환자_list 시트에 '병원등록번호' 또는 'hospital_id' 컬럼이 없습니다."
                )

        # 병원등록번호를 8자리 문자열로 변환
        df_demo["병원등록번호"] = df_demo["병원등록번호"].apply(
            lambda x: str(x).zfill(8) if pd.notnull(x) else ""
        )
        # 현재 pid와 일치하는 환자 정보 필터링
        df_demo_filtered = df_demo[df_demo["병원등록번호"] == pid]
        st.session_state.df_patient_demographic = df_demo_filtered

        # (B) 약물 데이터 로드: "내가먹는약_조회" 시트 사용
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
            # 병원등록번호를 8자리 문자열로 변환 후 현재 환자 데이터 필터링
            df_mydrug_raw["병원등록번호"] = df_mydrug_raw["병원등록번호"].apply(
                lambda x: str(x).zfill(8)
            )
            df_mydrug_filtered = df_mydrug_raw[df_mydrug_raw["병원등록번호"] == pid]

        # 약물 데이터가 존재하면 전처리 후 세션 상태에 저장, 없으면 경고 출력
        if not df_mydrug_filtered.empty:
            st.session_state.df_mydrug = process_medication_data(df_mydrug_filtered)
        else:
            st.warning("해당 환자에 대한 약물 데이터가 없습니다.")
            st.session_state.df_mydrug = pd.DataFrame()

        # (C) 진단 데이터 로드: DB 조회 후, 실패 시 구글 시트 fallback 사용 ("진단명" 시트)
        df_diagnosis_filtered = load_diagnosis_data(pid, doc)
        if not df_diagnosis_filtered.empty:
            st.info("진단 데이터를 불러왔습니다.")
        else:
            st.warning("해당 환자 진단명이 (DB+시트) 모두 없음.")
        st.session_state.df_mydiagnosis = df_diagnosis_filtered

        # (D) ADR 데이터 로드: "ADR_list" 시트 사용
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
# (8) 사이드바: 환자 Demographics 표시 및 수정
# ---------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("Patient Demographics")

if not st.session_state.df_patient_demographic.empty:
    # 환자 정보가 로드되어 있을 경우, 첫 번째 행을 기본 값으로 사용
    pat_info = st.session_state.df_patient_demographic.iloc[0]
    name = st.sidebar.text_input("성명", value=pat_info.get("성명", ""))
    research_id = st.sidebar.text_input(
        "연구등록번호", value=pat_info.get("연구등록번호", "")
    )
    hospital_id = st.sidebar.text_input(
        "병원등록번호", value=pat_info.get("병원등록번호", "")
    )
    birth_date = st.sidebar.text_input(
        "생년월일_new", value=pat_info.get("생년월일_new", "")
    )

    # 성별이 M 또는 F가 아니면 기본값 M 설정
    raw_gender = pat_info.get("성별", "M")
    if raw_gender not in ["M", "F"]:
        raw_gender = "M"
    gender = st.sidebar.selectbox(
        "성별", ["M", "F"], index=["M", "F"].index(raw_gender)
    )

    if st.sidebar.button("Update Patient Info"):
        # 수정된 값들을 session_state의 df_patient_demographic DataFrame에 업데이트
        st.session_state.df_patient_demographic.at[0, "성명"] = name
        st.session_state.df_patient_demographic.at[0, "연구등록번호"] = research_id
        st.session_state.df_patient_demographic.at[0, "병원등록번호"] = hospital_id
        st.session_state.df_patient_demographic.at[0, "생년월일_new"] = birth_date
        st.session_state.df_patient_demographic.at[0, "성별"] = gender
        st.sidebar.success("환자 정보가 업데이트되었습니다.")
else:
    st.sidebar.info("데이터 로드 후 환자정보가 표시됩니다.")


# ---------------------------------------------------------------------
# (9) Drug Selection: 약물 선택 관련 UI 및 기능 함수들
# ---------------------------------------------------------------------
def show_medication_data():
    """
    약물 데이터 전체를 DataFrame 형태로 화면에 출력.
    - 데이터가 없을 경우, 정보 메시지 출력.
    """
    st.subheader("Medication Data (All)")
    if st.session_state.df_mydrug.empty:
        st.info("No medication data.")
    else:
        st.dataframe(st.session_state.df_mydrug)


def show_current_medication():
    """
    현재 복용 중인 약물을 필터링하는 UI 기능.
    - 1/3 이상 처방된 약물과 1/3 미만 처방된 약물을 구분하여 표시.
    - 사용자가 선택한 약물 코드를 기반으로 최종 선택된 약물 데이터를 session_state에 저장.
    """
    st.subheader("Current Medication")
    df = st.session_state.df_mydrug.copy()
    if df.empty:
        st.warning("약물 데이터가 없습니다.")
        return

    df["약품코드"] = df["약품코드"].astype(str)
    # 각 약품코드에 대해 전체 처방 일수를 계산 (med_days)
    df["med_days"] = df.groupby("약품코드")["일수"].transform("sum")

    total_duration = df["duration"].iloc[0] if len(df) > 0 else 0
    one_third = total_duration / 3.0
    st.write(f"총 duration: {total_duration}, 1/3 = {one_third:.1f}")

    # 1/3 이상과 미만 처방된 약물을 각각 구분하여 표시
    df_above = df[df["med_days"] >= one_third].drop_duplicates(subset=["약품코드"])
    df_below = df[df["med_days"] < one_third].drop_duplicates(subset=["약품코드"])

    st.markdown("#### [1/3 이상 처방 약물]")
    if df_above.empty:
        df_above_disp = pd.DataFrame(columns=["약품코드", "약품명", "med_days"])
        df_above_disp["select"] = False
    else:
        df_above["select"] = True  # 기본적으로 선택된 상태(True)
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
        # 선택된 약물 코드를 결합하여 최종 현재 복용 약물 리스트 생성
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

        final_codes = list(set(final_codes))  # 중복 제거
        final_df = df[df["약품코드"].isin(final_codes)].drop_duplicates(subset=["약품코드"])

        # 최종 선택된 약물 데이터에서 필요한 컬럼만 추출
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
    """
    신규 처방약물을 날짜별로 선택하는 기능.
    - 사용자가 날짜를 선택하면 해당 날짜에 처방된 약물 데이터를 보여주고,
      사용자가 선택한 약물들을 최종 신규 처방 데이터로 저장.
    """
    st.subheader("Newly Prescription")
    df = st.session_state.df_mydrug.copy()
    if df.empty:
        st.warning("약물 데이터가 없습니다.")
        return

    # 조제일자를 datetime 형식으로 재변환 (혹시 오류가 있을 경우 대비)
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

    # 필요한 컬럼들을 선택 및 존재하지 않는 컬럼은 기본값으로 추가
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
# (10) '진단명 추가' 알고리즘(Comorbidity Algorithm)
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
    """
    현재 선택된 약물 데이터(df_current)를 기반으로 comorbidity (동반질환) 진단을 추론합니다.
    - 기존의 진단 데이터(session_state.df_mydiagnosis)와 중복되지 않는 ICD10 코드를 추출
    - 각 약물의 ATC 코드가 comorbidity_map에 정의된 ATC prefix와 일치하면 해당 ICD10 정보를 결과에 추가
    """
    if df_current.empty:
        return pd.DataFrame()

    # 이미 존재하는 ICD10 코드 집합 (중복 방지)
    existing_codes = (
        set(st.session_state.df_mydiagnosis["condition_source_value"].astype(str))
        if not st.session_state.df_mydiagnosis.empty
        else set()
    )

    results = []
    for _, row in df_current.iterrows():
        drugName = row.get("약품명", "")
        atcStr = row.get("atc_code_str", "")
        # 쉼표로 구분된 ATC 코드 문자열을 리스트로 분할 후 공백 제거
        atc_list = [s.strip() for s in atcStr.split(",") if s.strip()]

        # 각 ATC 코드에 대해 comorbidity_map에 정의된 질환과 매칭 검사
        for atc_single in atc_list:
            for disease_name, info in comorbidity_map.items():
                prefixes = info["atc_prefixes"]
                icd_info = info["icd_info"]
                if any(atc_single.startswith(pref) for pref in prefixes):
                    code_val = icd_info["condition_source_value"]
                    if code_val not in existing_codes:
                        results.append(
                            {
                                "select": False,  # 초기 선택 상태 False
                                "drugName": drugName,
                                "atcCode": atc_single,
                                # concept_name에 해당하는 진단명을 icd_info에서 설정
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
    진단 선택 관련 전체 UI를 구성하는 함수.
    - "진단명 조회" 탭: 기존 진단 데이터를 확인하고 선택할 수 있음.
    - "진단명 추가" 탭: comorbidity 알고리즘을 통해 추론된 추가 진단 데이터를 확인 후 선택할 수 있음.
    """
    st.title("Diagnosis Selection")
    diag_subtabs = st.tabs(["진단명 조회", "진단명 추가"])

    # (A) 진단명 조회 탭
    with diag_subtabs[0]:
        st.subheader("진단명 조회")

        df_diag_raw = st.session_state.df_mydiagnosis.copy()
        if df_diag_raw.empty:
            st.warning("해당 환자에 대한 진단 기록이 없습니다 (DB+시트 모두 조회 실패).")
            return

        # concept_name과 condition_source_concept_id가 따로 있을 경우, 통합하여 최종 진단명을 생성
        if "concept_name" not in df_diag_raw.columns:
            df_diag_raw["concept_name"] = ""

        if "condition_source_concept_id" in df_diag_raw.columns:
            for idx, row in df_diag_raw.iterrows():
                if not row["concept_name"]:
                    df_diag_raw.at[idx, "concept_name"] = row["condition_source_concept_id"]
            df_diag_raw.drop(columns=["condition_source_concept_id"], inplace=True)

        # 선택을 위한 select 컬럼 추가 (기본값 False)
        if "select" not in df_diag_raw.columns:
            df_diag_raw["select"] = False

        # 보여줄 컬럼 순서 재정렬 (select, condition_source_value, concept_name 우선)
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
                # NaN 값을 빈 문자열로 변환
                chosen.fillna("", inplace=True)

                keep_cols = ["condition_source_value", "concept_name"]
                exist_cols = [c for c in keep_cols if c in chosen.columns]
                chosen_clean = chosen[exist_cols].drop_duplicates()

                st.session_state.df_mydiagnosis = chosen_clean.reset_index(drop=True)
                st.success("진단명 조회 Update 완료.")
                st.dataframe(st.session_state.df_mydiagnosis)

    # (B) 진단명 추가 탭 (Comorbidity Algorithm)
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
# (11) ADR Selection: 부작용(ADR) 선택 관련 UI 및 기능 함수
# ---------------------------------------------------------------------
def show_adr_selection():
    """
    ADR(부작용) 데이터를 표시하고, 사용자가 선택할 수 있도록 UI를 구성합니다.
    - 'ADR_list' 시트에서 로드한 데이터를 기반으로, 각 ADR 항목에 대해 선택 옵션 제공
    - 선택된 ADR 항목은 후속 JSON 출력에 포함됩니다.
    """
    st.title("ADR Selection")
    df_adr = st.session_state.df_myadrs.copy()
    if df_adr.empty:
        st.info("이 환자의 ADR 데이터가 없습니다.")
        return

    # ADR 데이터를 하나의 문자열(combined_adr)로 결합하여 표시
    df_adr["combined_adr"] = df_adr.apply(
        lambda row: (
            f"{row.get('ADR_annocation','')} ({row.get('action_plan_for_tolerable_drugs','')})"
            if pd.notnull(row.get("ADR_annocation", "")) and pd.notnull(row.get("action_plan_for_tolerable_drugs", ""))
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

    # ADR 관련 여러 컬럼들을 순서대로 배치하여 데이터프레임 구성
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
# (12) Final JSON Output: 최종 JSON 결과 생성 및 구글 스프레드시트 업로드 기능
# ---------------------------------------------------------------------
def show_final_json():
    """
    환자 Demographics, 진단, 약물, ADR 데이터를 바탕으로 최종 JSON 구조를 생성합니다.
    JSON 구조는 다음 키를 포함:
      - age: 환자 나이
      - gender: 환자 성별
      - comorbidities: 진단(동반질환) 목록 (ICD10 코드 기반)
      - currentMedication: 현재 복용 중인 약물 목록
      - newPrescriptions: 신규 처방 약물 목록
      - adrs: 선택된 ADR 항목 목록
    생성된 JSON은 화면에 출력되며, "output sheet upload" 버튼 클릭 시
    구글 스프레드시트의 여러 시트에 결과가 업데이트됩니다.
    """
    st.title("Final JSON Output")

    # 환자 정보 로드 (Demographics)
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

    # (A) comorbidities: 선택된 진단명 (ICD10 코드 기반) 처리
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
                        "diagnosisName": dname,  # concept_name을 진단명으로 사용
                    }
                )

    # (B) currentMedication: 현재 복용 중인 약물 처리
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

    # (C) newPrescriptions: 신규 처방 약물 처리
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

    # (D) adrs: 선택된 ADR 데이터 처리 (JSON에는 description만 포함)
    adrs = []
    if not st.session_state.chosen_adr.empty:
        df_adr_sel = st.session_state.chosen_adr.copy().fillna("")
        for _, row in df_adr_sel.iterrows():
            combined_adr = str(row.get("combined_adr", "")).strip()
            if combined_adr:
                adrs.append({"description": combined_adr})

    # 최종 JSON 구조 생성
    final_output = {
        "age": final_age,
        "gender": final_gender,
        "comorbidities": comorbidities,
        "currentMedication": currentMedication,
        "newPrescriptions": newPrescriptions,
        "adrs": adrs,
    }
    st.session_state.final_output_json = final_output

    st.json(final_output)  # JSON 결과를 화면에 출력

    # 구글 스프레드시트 출력 시트에 업로드 기능
    if st.button("output sheet upload"):
        doc_out = gc.open_by_url(output_spreadsheet_url)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # (1) baseline_characteristics 시트 업데이트
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

        # (2) current_medication 시트 업데이트
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

        # (3) newly_prescription 시트 업데이트
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

        # (4) diagnosis 시트 업데이트
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

        # (5) adr_selected 시트 업데이트
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

        # (6) patient_dataset_generation 시트 업데이트: 최종 JSON 결과 전체 저장
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
# (13) 메인 탭: 전체 UI를 탭으로 구성하여 약물, 진단, ADR, 최종 JSON 출력 제공
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
    # 약물 선택 관련 하위 탭 구성: 전체 약물 데이터, 현재 복용 약물, 신규 처방 약물
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
