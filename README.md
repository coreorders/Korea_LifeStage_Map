# kids_birth data sync

행정안전부 OpenAPI(공공데이터포털)에서 법정동 기반 성/연령별 주민등록 인구 데이터를 수집해 SQLite DB에 저장합니다.

## API 키 필요 여부

필요합니다. 아래 OpenAPI는 `serviceKey`가 필수이며, 키 없이 호출하면 `401 Unauthorized`가 반환됩니다.

- Endpoint: `https://apis.data.go.kr/1741000/stdgSexdAgePpltn/selectStdgSexdAgePpltn`
- Dataset page: `https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15108074`

## 로컬 실행

```bash
# Windows PowerShell
$env:PUBLIC_DATA_API_KEY="발급받은_서비스키"
python scripts/sync_population.py --auto-month --only-new
```

옵션:

- `--month YYYYMM` 대상 월 (기본: 전월)
- `--auto-month` API에서 최신 데이터가 있는 월을 자동 탐색
- `--only-new` DB에 이미 같은/더 최신 월이 있으면 스킵
- `--stdg-cd` 법정동 코드 (기본: `0000000000`)
- `--lv` 조회레벨 (기본: `3`)
- `--save-raw` 원본 페이지 JSON 저장
- `--max-pages N` 테스트용 페이지 제한

## DB 스키마

`data/population.db`에 아래 2개 테이블을 사용합니다.

- `sync_runs`: 실행 이력(성공/실패, 건수)
- `population_items`: 월별 원본 item JSON 저장(중복 key는 upsert)

## GitHub Actions

- 파일: `.github/workflows/weekly-sync.yml`
- 스케줄: 매일 02:00 UTC
- 필요 secret: `PUBLIC_DATA_API_KEY`
- 실행 후 `data/population.db` 변경이 있으면 자동 커밋/푸시

## API 키 발급 (네가 할 일)

1. 공공데이터포털 로그인: `https://www.data.go.kr/`
2. 데이터 페이지 이동: `https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15108074`
3. `활용신청` 버튼 클릭
4. 앱 이름/용도 입력 후 신청 완료
5. 마이페이지에서 `일반 인증키(Decoding)` 확인/복사
6. GitHub 저장소 `Settings > Secrets and variables > Actions > New repository secret`
7. 이름 `PUBLIC_DATA_API_KEY`, 값에 인증키 붙여넣기
