# Streamlit 애플리케이션: 환자 데이터 처리

이 애플리케이션은 **Google Sheets**와 **PostgreSQL** 데이터베이스를 연동하여, 환자 정보를 효율적으로 불러오고 처리하는 **Streamlit** 기반 웹 애플리케이션입니다. 환자 인구통계 정보, 약물 정보, 진단 정보, 부작용(ADR) 정보 등을 종합적으로 조회·저장·분석할 수 있습니다.

## 주요 기능
1. **Google Sheets 연동**  
   - `gspread` 라이브러리를 사용해 Google Sheets와 연동합니다.  
   - 입력용/출력용 Google Sheets URL을 설정하여 원하는 시트를 참조 및 업데이트할 수 있습니다.

2. **PostgreSQL 데이터베이스 연동**  
   - `SQLAlchemy`를 활용해 PostgreSQL에 쿼리를 수행합니다.  
   - 진단 정보를 SNOMED→ICD10으로 매핑하기 위해 OMOP CDM 테이블(예: `condition_occurrence`)을 조회하고, 부족 시에 Google Sheets 데이터를 보완적으로 사용합니다.

3. **환자 Demographics 처리**  
   - Google Sheets(예: `환자_list` 시트)에 저장된 환자 인구통계 정보를 불러옵니다.  
   - 환자 정보를 애플리케이션 내에서 조회·수정하여 세션에 반영할 수 있습니다.

4. **약물 정보 전처리 및 선택**  
   - Google Sheets(예: `내가먹는약_조회` 시트)에서 약물 데이터를 불러와 SQLAlchemy를 통해 ATC 코드를 매핑합니다.  
   - 처방일, 투약 일수, 성분명, 약품명 등을 확인할 수 있으며, 현재 복용약물 목록과 신규 처방약물 목록을 따로 선택·관리할 수 있습니다.

5. **진단 정보 조회/추가**  
   - DB(OMOP CDM)에 실제 진단 정보(SNOMED→ICD10 매핑 결과)가 있으면 불러오고, 없을 경우 Google Sheets(예: `진단명` 시트)에 있는 진단 정보로 대체합니다.  
   - **Comorbidity Algorithm(동반질환 추론):** 현재 복용약물의 ATC 코드에 따라 고혈압·당뇨병·이상지질혈증 등을 자동 추론하여 ICD10 코드를 제안합니다.

6. **ADR(부작용) 정보 처리**  
   - Google Sheets(`ADR_list` 시트)에서 불러온 부작용 정보를 표시합니다.  
   - 사용자에게 필요한 항목을 선택시키고, 선택된 ADR을 후속 JSON 출력에 포함합니다.

7. **최종 JSON 구조 생성 및 업로드**  
   - 환자 나이, 성별, 동반질환(ICD10), 현재복약, 신규처방, 부작용 등의 정보를 구조화한 JSON을 생성합니다.  
   - 그 결과를 다른 Google Sheets(`baseline_characteristics`, `current_medication`, `diagnosis` 등)에 업로드하여 기록을 남길 수 있습니다.

---

## 사전 준비

- **Python 버전:** 3.13  
- **패키지:**  
  - `streamlit`, `pandas`, `gspread`, `google-auth`, `sqlalchemy`, `psycopg2`(또는 `asyncpg` 등 PostgreSQL 드라이버)  
- **데이터베이스:** PostgreSQL 접근 권한 및 DB 설정  
- **Google OAuth2 자격 증명(서비스 계정 JSON 파일)**

### Google Sheets 권한 설정
1. Google Cloud 콘솔에서 서비스 계정 생성 및 키(JSON) 다운로드.  
2. 해당 서비스 계정에 Google Drive/Sheets 접근 권한을 할당해야 합니다.  
3. 스크립트 내부(`json_file_name`)에 JSON 파일 경로를 지정하고, `scopes` 리스트를 필요에 따라 수정합니다.

---

## 설치 및 실행

1. **프로젝트 클론** (또는 다운로드 후 폴더로 이동)
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```
2. **필요 패키지 설치**
   ```bash
   pip install streamlit pandas gspread google-auth sqlalchemy
   ```
3. **서비스 계정 JSON 파일 준비**
   - 스크립트(`streamlit_test_주석.py`) 내 `json_file_name`에 파일명을 맞춰주세요.
   - 같은 폴더에 JSON 파일을 배치하거나, 절대경로를 추가로 설정 가능합니다.
4. **데이터베이스 연결 정보(DB_PARAMS, DB_PARAMS_OHDSI) 수정**
   - `host`, `port`, `dbname`, `user`, `password` 등을 실제 서버 환경에 맞게 설정해주세요.

5. **애플리케이션 실행**
   ```bash
   streamlit run streamlit_test_주석.py
   ```
   브라우저에서 `http://localhost:8501`로 접속하면 됩니다.

---

## 사용 방법

### 1. 사이드바 입력
- **구글 스프레드시트 URL 입력:**  
  - 입력용(환자_list, 내가먹는약_조회 등)  
  - 출력용(baseline_characteristics, current_medication 등)  
- **환자등록번호 입력:** 8자리 숫자로 자동 변환됩니다.  

### 2. "Load Data" 버튼
- **Google Sheets**에서 환자 인구통계, 약물정보, ADR 정보를 불러오고,  
- **OMOP DB**에서 SNOMED→ICD10 매핑 진단 정보를 조회합니다.  
- 실패 시 구글 시트(`진단명`)에서 진단정보를 대체로 사용합니다.

### 3. 약물 선택(Drug Selection)
- **Medication Data 탭:** 전체 약물 테이블 확인.  
- **Current Medication 탭:** 1/3 이상/미만 처방된 약을 필터링하고, 체크하여 현재 복용약물 확정.  
- **Newly Prescription 탭:** 날짜별로 처방된 신규 약물을 확인하고 선택·확정.

### 4. 진단 선택(Diagnosis Selection)
- **진단명 조회:** DB 혹은 시트에서 불러온 진단 리스트를 편집(Data Editor)하여 필요한 항목 체크 → 업데이트.  
- **진단명 추가(Comorbidity):** 현재 약물의 ATC 코드로부터 고혈압, 당뇨병, 이상지질혈증 등을 자동 추론.

### 5. ADR 선택(ADR Selection)
- ADR 리스트를 표시하고, 체크를 통해 필요한 부작용 항목을 선택.

### 6. 최종 JSON 출력(Final JSON Output)
- **Demographics, 진단, 약물, ADR**를 모두 반영한 JSON을 화면에 표시.  
- "output sheet upload" 버튼으로 결과를 출력 Sheets에 반영.

---

## 주요 구현 로직

1. **DatabaseManager 클래스**  
   - `with` 문을 통해 PostgreSQL 연결을 쉽게 관리할 수 있도록 구현.  
   - `execute_query` 메소드를 이용해 쿼리 결과를 `pandas.DataFrame`으로 반환.

2. **약물 데이터 전처리(`process_medication_data`)**  
   - 약품코드(숫자 → 문자열 전환 및 zero-fill)  
   - 일수 계산, duration(최초·최종 처방일 차이)  
   - DB를 통해 ATC 코드 매핑 후, 쉼표 구분 문자열(`atc_code_str`) 생성

3. **OMOP DB 진단 조회**  
   - `person_mapping_key.xlsx` → 환자등록번호(hospital_id)를 OMOP `person_id`로 변환  
   - `condition_occurrence` 및 `concept` 테이블을 조인하여 SNOMED→ICD10 맵핑된 진단 정보를 가져옴  
   - 중복된 진단 코드는 정리 후, 시트 진단과 병합

4. **Comorbidity Algorithm**  
   - ATC 코드 Prefix(C02, A10, C10 등)를 분석  
   - 매칭된 항목이 기존 진단에 없으면 추가할 수 있도록 UI 제공

5. **ADR 처리**  
   - 시트 `ADR_list` 로부터 부작용(ADR) 데이터 불러오기  
   - 사용자에게 필요한 ADR만 선택할 수 있도록 Data Editor 제공

6. **최종 스프레드시트 업로드**  
   - `baseline_characteristics`, `current_medication`, `diagnosis`, `adr_selected`, `patient_dataset_generation` 시트 등에 각각 해당하는 정보를 append.

---

## 문제 해결(Troubleshooting)

1. **구글 인증 실패**  
   - JSON 파일 경로 혹은 파일명이 맞는지 확인  
   - 서비스 계정에 시트 공유권한이 있는지 확인  
2. **데이터베이스 연결 불가**  
   - Host, Port, DB명, 계정 정보가 올바른지, DB 서버가 실제로 열려 있는지 확인  
3. **OMOP DB 오류**  
   - 테이블/스키마명이 실제와 일치하는지 확인 (예: `cbnuh_omop_cdm`, `vocab_202210` 등)  
4. **Streamlit이 적절히 실행되지 않음**  
   - `pip list`로 streamlit 버전을 확인 (필요 시 재설치)

---

## 라이선스

이 프로젝트는 MIT 라이선스를 따르며, 자세한 내용은 [LICENSE](LICENSE) 파일을 참고하세요. 