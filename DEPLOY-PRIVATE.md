# 본인 전용 서버 배포

이 구성은 **서버 1대 + Docker Compose + SSH 터널**을 사용합니다. 웹 포트는 서버의 `127.0.0.1`에만 연결됩니다. 개인 PC에서 SSH 키로 인증한 뒤 대시보드를 엽니다. 도메인이나 웹 로그인 계정은 필요하지 않습니다. 이 문서와 실행 파일은 배포 준비물이며 실제 호스팅 계정이나 서버를 생성하지 않습니다.

## 필요한 서버

- 권장 시작 사양: Ubuntu 24.04 LTS, 1 vCPU, RAM 2GB, SSD 20GB 이상. 수집량에 따라 증설합니다.
- Docker Engine **28 이상**과 Docker Compose v2. 공식 설치 문서: https://docs.docker.com/engine/install/ubuntu/
- 본인만 사용하는 SSH 계정과 SSH 키. 비밀키는 개인 PC에 보관합니다.
- 클라우드 방화벽의 인바운드는 SSH 포트만 허용합니다. 가능하면 본인의 접속 IP로 제한합니다. **8000 포트는 열지 않습니다.** 아웃바운드는 공개 HTTPS 수집을 허용합니다.
- 서버는 계속 켜져 있어야 합니다. PC에서 터널이나 브라우저를 닫아도 서버의 수집은 계속됩니다.

서버/호스팅 계정이 없다면 먼저 위 사양의 VPS를 준비해야 합니다. 공급자의 비용, 리전과 SSH 접속 가능 여부를 확인하고 선택하세요. 이 소프트웨어가 서버를 자동 구매하지는 않습니다.

## 처음 실행

소스 폴더 전체를 서버의 본인 작업 폴더로 복사합니다. 기존 로컬 사용 기록을 옮기려면 아래 **기존 로컬 데이터 이전**부터 진행하세요.

서버의 소스 폴더에서:

```sh
docker compose config --quiet
docker compose build --pull
docker compose up -d
docker compose ps
docker compose logs --tail=50 sentinel backup
```

`sentinel`과 `backup`이 실행되고 잠시 후 healthy로 표시되어야 합니다. 첫 백업은 앱의 정상 시작 후 만들어집니다. 빌드에는 Python 패키지 다운로드를 위한 인터넷이 필요합니다. 앱은 일반 사용자 UID 10001로 실행되며 DB와 백업은 서로 다른 Docker 볼륨에 저장됩니다.

기본 포트 8000이 사용 중이라면 소스 폴더의 `.env` 파일에 `QA_SENTINEL_PORT=8001`을 저장하고 다시 `docker compose up -d`를 실행합니다. 호스트 바인딩 `127.0.0.1`은 유지하세요. 컨테이너 내부의 `0.0.0.0` 수신은 정상입니다.

## 개인 PC에서 접속

개인 PC의 PowerShell 또는 터미널에서 아래 `USER`와 `SERVER_IP`를 본인 서버 계정과 IP로 바꿉니다.

```sh
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -L 127.0.0.1:8000:127.0.0.1:8000 USER@SERVER_IP
```

SSH 연결이 유지되는 동안 개인 PC 브라우저에서 **http://127.0.0.1:8000**을 엽니다. 트래픽은 암호화된 SSH 연결로 서버에 전달됩니다. 터널 종료는 `Ctrl+C`입니다. PC와 서버 포트를 다르게 쓰려면 `-L 127.0.0.1:8001:127.0.0.1:8000`으로 PC 쪽 포트만 바꿀 수 있습니다.

처음에는 샘플 화면입니다. **실제 수집**으로 전환하고 채널/사전/임계치를 설정합니다. **지금 수집**으로 대상 게시판에 접근 가능한지 확인합니다. 데이터센터 IP에 대한 사이트의 차단 여부는 실제 서버에서 확인해야 합니다. 디시인사이드 차단 해제 기능은 포함되지 않습니다.

## 상태 확인과 재시작

```sh
docker compose ps
docker compose exec sentinel python scripts/container_health.py
docker compose exec backup python -m qa_sentinel.backup check --backup-dir /backups
docker compose logs --tail=100 sentinel backup
docker compose restart sentinel
```

앱 종료와 서버 재부팅 후에는 Docker의 `unless-stopped` 정책에 따라 다시 실행됩니다. Docker 자체도 재부팅 후 시작되도록 설정하세요. 관리자가 명시적으로 정지한 서비스는 `docker compose up -d`로 재개합니다. healthcheck는 HTTP 응답과 최근 스케줄러 실행을 검사합니다. unhealthy 상태 자체가 자동 재시작이나 외부 알림을 발생시키지는 않습니다. 원인을 해결하고 재시작하세요. 메신저 알림은 후속 개발 항목입니다.

## 자동 백업과 별도 보관

백업 서비스는 시작 직후, 이후 24시간 간격으로 SQLite의 온라인 백업 API를 사용합니다. WAL에 남아 있는 커밋도 포함하며 실제 수집/샘플 DB, 사전, 임계치, 채널, 이벤트를 보존합니다. DB별 스냅샷이며 두 DB 간 전역 시점 일치는 보장하지 않습니다. 로그는 포함하지 않습니다. 최근 **14개 스냅샷**을 보관합니다. 실패하면 로그를 남기고 5분 뒤 재시도합니다. 최근 완료 백업이 26시간 이상 없으면 백업 healthcheck가 실패합니다.

수동 백업 및 목록:

```sh
docker compose run --rm maintenance python -m qa_sentinel.backup create --data-dir /data --backup-dir /backups
docker compose run --rm maintenance
```

서버에서 백업 파일을 꺼냅니다. `BACKUP_FILE.zip`은 위 목록에 나온 실제 파일명으로 바꿉니다.

```sh
mkdir -p backups
docker compose cp backup:/backups/BACKUP_FILE.zip ./backups/
```

그다음 개인 PC에서 `scp USER@SERVER_IP:서버의_소스폴더/backups/BACKUP_FILE.zip .`로 가져옵니다. 서버와 다른 장치에 사본을 보관하세요. Docker 백업 볼륨도 서버 디스크 손실 시 함께 잃을 수 있습니다. 백업 ZIP은 암호화되어 있지 않으므로 본인만 접근하는 위치에 보관합니다.

## 복구

현재 자료를 먼저 백업하고 앱과 자동 백업을 모두 멈춥니다. 아래 파일명을 실제 백업 파일명으로 바꿉니다.

```sh
docker compose stop sentinel backup
docker compose run --rm maintenance python -m qa_sentinel.backup restore --data-dir /data --archive /backups/BACKUP_FILE.zip --confirm-restore
docker compose up -d
```

복구는 ZIP 구성, SHA-256과 SQLite 무결성을 확인한 뒤 교체합니다. 앱이 실행 중이면 거부합니다. 직전 DB는 데이터 볼륨의 `pre-restore-...` 폴더에 보관합니다. 복구 후 실제 수집 목록, 사전과 채널을 확인하세요. 이전 자료는 확인 후 정리할 수 있습니다. 복구는 현재 DB를 백업 시점으로 교체하므로 이후 글과 설정 변경은 현재 DB에서 사라집니다.

개인 PC에만 있는 ZIP을 복구하려면 먼저 서버로 복사하고, `backup` 서비스가 실행 중일 때 `docker compose cp ./BACKUP_FILE.zip backup:/backups/`로 넣습니다. 그런 다음 위 정지/복구 절차를 따릅니다. 앱과 동일하거나 호환되는 버전에서 생성한 백업을 사용하세요.

## 기존 로컬 데이터 이전

로컬 프로그램과 해당 데이터 폴더를 쓰는 모든 서버를 종료합니다. 로컬 소스 폴더에서:

```sh
python -m qa_sentinel.backup create --data-dir .sentinel-data --backup-dir backups
```

출력된 ZIP을 새 서버로 복사합니다. 새 서버에서 `docker compose up -d`로 빈 DB와 볼륨을 초기화하고, ZIP을 백업 서비스의 `/backups/`에 넣은 뒤 위 복구 절차를 따릅니다. 로그와 서버 잠금 파일은 이전할 필요가 없습니다. 원본 로컬 폴더는 서버 검증이 끝날 때까지 보관하세요.

## 업데이트와 중지

업데이트 전 수동 백업을 생성합니다. 앱을 정지하고 새 소스로 교체한 후 실행합니다.

```sh
docker compose stop sentinel backup
docker compose build --pull
docker compose up -d
```

실행 후 상태/로그와 수집을 확인하세요. `docker compose down`은 컨테이너와 네트워크를 제거하며 데이터 볼륨은 남깁니다. **`docker compose down -v`는 DB와 백업 볼륨을 모두 삭제하므로 사용하지 마세요.** 운영 중 Compose 프로젝트 이름 `qa-sentinel`을 유지하세요. 이름을 바꾸면 다른 볼륨을 사용합니다.

## 서버에서 마무리할 검증

1. SSH 터널로 본인 접속, 서버 공인 IP의 웹 포트가 외부 비공개인지 확인합니다.
2. 실제 게시판 수집과 시간대, 키워드/주의/위험 판정을 확인합니다.
3. 컨테이너 및 서버 재시작 후 설정/글이 남는지 확인합니다.
4. 백업을 별도 장치로 복사하고 임시 환경에서 복구를 확인합니다.
5. 실제 24시간 운영 후 수집·오류 로그와 디스크 여유를 확인합니다.

로컬 자동 테스트나 컨테이너 검사는 위 실제 서버 검증을 대신하지 않습니다.
