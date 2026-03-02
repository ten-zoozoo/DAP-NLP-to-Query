# DAP (Data Access Platform)

의료 도메인 사용자가 **자연어로 질문하면 Oracle 의료 DB(MIMIC 기반)를 안전하게 조회**할 수 있도록, LLM이 SQL을 생성하고 결과를 UI로 제공하는 데이터 접근 플랫폼입니다.

## 핵심 기능

- **자연어 → SQL 자동 생성**
  - 한국어 질문을 의료 문맥에 맞게 번역/정규화한 뒤 SQL(Oracle dialect)을 생성합니다.
  - 리터럴은 바인드 변수로 변환해 저장합니다(재현 가능성 및 안전성 목적).
- **역할 기반 접근 제어 (RBAC) + 테이블 권한 검증**
  - `CLINICAL / RESEARCHER / ADMINISTRATION / SYSTEM` 역할에 따라 DB 계정 풀을 분리합니다.
  - 생성된 SQL에서 참조 테이블을 추출하고, **권한 없는 테이블이면 즉시 차단**합니다.
- **비동기 답변 생성 + 폴링**
  - 질문 입력 후 백그라운드 스레드에서 LLM 답변을 생성합니다.
  - 프론트는 `/check_answer_status/<answer_id>`를 폴링하여 완료 시 자동 갱신합니다.
- **결과 미리보기/상세보기**
  - 채팅 화면에서 최근 답변 일부를 테이블로 미리보기(제한 행 수)합니다.
  - 상세 팝업에서 SQL 포맷팅, 결과 테이블, “조건 및 로직(설명)”을 함께 제공합니다.
- **시각화 + CSV 다운로드**
  - 상세 팝업에서 축/그래프 타입을 선택해 Plotly 그래프를 생성합니다.
  - 결과를 CSV로 다운로드합니다(기본 최대 100,000행 제한).
- **즐겨찾기**
  - 생성된 질의를 타이틀로 저장하고 목록에서 일괄 삭제할 수 있습니다.
- **시스템 관리자 기능**
  - 시스템 모니터링(쿼리 통계/슬로우 쿼리 등)과 사용자 권한 관리 화면을 제공합니다.
- **개인정보/식별자 마스킹**
  - 결과에서 MIMIC 민감 컬럼(`SUBJECT_ID`, `HADM_ID`, `STAY_ID` 등)을 부분 마스킹합니다.
- **조회 전용(의도) 가드레일**
  - SQL 생성 파이프라인에서 `INSERT/UPDATE/DELETE/DROP/...` 등 금지 키워드를 차단합니다.
  - CSV 다운로드는 `SELECT`만 허용합니다.

## 화면 흐름 (요약)

1. 로그인: `/` (GET/POST)
2. 채팅 시작: `/new-chat`
3. 기존 세션 진입: `/main/<chat_session_id>` (GET/POST)
4. 답변 상태 확인: `/check_answer_status/<answer_id>`
5. 상세 결과 팝업: `/popup/<answer_id>` (GET/POST)
6. CSV 다운로드: `/popup/<answer_id>/download`
7. 즐겨찾기: `/favorite`
8. 시스템 로그(관리자): `/system_log`
9. 권한 관리(관리자): `/manage_role`

## 기술 스택

- Backend: Flask
- DB: Oracle Database (`oracledb` connection pool)
- LLM: Ollama 기반 로컬 LLM + RAG(ChromaDB)
  - Embedding: `sentence-transformers`
  - Vector DB: `chromadb` (persistent collections)
- SQL 파싱/포맷: `sqlglot`, `sqlparse`
- Auth: `passlib`(argon2)
- Visualization: Plotly

## 디렉터리 구조

- `app.py`: Flask 엔트리포인트(라우트, 세션, 채팅 플로우, 비동기 답변 생성)
- `func.py`: DB 액세스/즐겨찾기/권한검증/시각화 유틸
- `llm.py`: 자연어 → SQL 파이프라인(번역, 용어 매핑, 스키마 선택, 생성/검증, 마스킹)
- `templates/`: `login.html`, `main.html`, `popup.html`
- `static/`: `main.css`, `popup.css`, `login.css`, `script.js`, `image/`
- `for_llm/`: 테이블/개념/문법 정보를 위한 데이터 및 Chroma DB 생성 노트북/리소스
- `mimic_medical_word_json/`: 의료 용어 매핑용 JSON 리소스
- `dba_work/`: DBA 분석/설계 산출물(ERD 등)

## 실행 방법 (로컬)

### 1) 환경 준비

- Python 가상환경 생성 후 의존성 설치를 권장합니다.

> 참고: 현재 `requirements.txt`는 `UTF-16 LE` 인코딩입니다. 일부 환경에서 `pip install -r requirements.txt`가 실패할 수 있어, 아래처럼 UTF-8로 변환 후 설치하는 방식을 권장합니다.

```bash
python3 -m venv .venv
source .venv/bin/activate

# requirements.txt(UTF-16) -> requirements.utf8.txt(UTF-8)
python3 - <<'PY'
from pathlib import Path
p = Path("requirements.txt")
txt = p.read_text(encoding="utf-16")
Path("requirements.utf8.txt").write_text(txt, encoding="utf-8")
print("wrote requirements.utf8.txt")
PY

pip install -r requirements.utf8.txt
```

### 2) `.env` 설정

`app.py`는 다음 환경변수를 참조합니다.

```dotenv
# Oracle 접속 정보
ORACLE_DSN=host:port/service_name
user=...
password=...
```

### 3) Oracle Instant Client 설정

현재 코드는 Thick 모드 초기화를 위해 Oracle Instant Client 경로를 하드코딩하고 있습니다.

- `app.py`: `oracledb.init_oracle_client(lib_dir=...)`
- `llm.py`: `Config.ORACLE_CLIENT_PATH`

운영/개발 환경(Windows/macOS/Linux)에 맞게 경로를 조정하거나, 배포 시에는 환경변수 기반으로 분리하는 것을 권장합니다.

### 4) LLM/RAG 리소스 경로 설정

`llm.py`는 ChromaDB 및 약어/스키마 리소스 경로를 Windows 절대경로로 가정하고 있습니다.

- `CHROMA_DB_FOLDER`
- `Config.TABLE_INFO_PATH / CONCEPT_PATH / SYNTAX_INFO_PATH`
- `Config.ABBR_DICT_PATH`

이 저장소 구조에 맞춰 로컬 실행 시에는 아래처럼 **프로젝트 상대경로(예: `./for_llm`)로 변경**하는 것을 권장합니다.

```python
# llm.py (예시)
CHROMA_DB_FOLDER = r"./for_llm"
# ABBR_DICT_PATH = r"./for_llm/mimic_iv_abbreviation_160.json"
```

### 5) 실행

```bash
python3 app.py
```

브라우저에서 `http://127.0.0.1:5000/` 로 접속합니다.

## 기획 및 퍼블리싱 기여

본 프로젝트에서 저는 **서비스 기획 및 퍼블리싱(프론트 UI 구현)** 을 담당했습니다.

- 사용자 시나리오 설계: “질문 입력 → 생성 중 상태 → 결과 미리보기 → 상세(쿼리/로직/시각화)”
- 화면/정보 구조(IA) 구성: 사이드바(메뉴/최근 기록), 채팅 중심 레이아웃, 관리자 전용 대시보드 분리
- 퍼블리싱 구현:
  - 로그인/메인/팝업 화면 템플릿: `templates/`
  - 스타일링 및 인터랙션: `static/*.css`, `static/script.js`
  - 사용성 포인트: 폴링 기반 자동 갱신, 즐겨찾기 일괄 선택/삭제, 팝업 상세보기, 그래프 조건 선택 UI

## 보안/운영 주의사항

- 현재 저장소에는 실행 편의를 위한 값(예: `app.secret_key`, DB 계정/비밀번호 등)이 코드에 존재합니다. **외부 공개/실서비스 배포 전 반드시 제거하고 Secret 관리로 전환**하세요.
- 본 프로젝트는 조회 중심(SELECT)으로 설계되어 있으나, 실제 운영 시에는 DB 권한/네트워크/감사 로깅 등 추가적인 보안 통제가 필요합니다.
