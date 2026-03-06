# beflex-support

Repository นี้คัดเฉพาะส่วน **beflex-support service** เท่านั้น (ไม่มีไฟล์ระบบอื่นจาก beflex-workspace)

## Included
- `beflex-support-backend/` (Node.js API)
- `beflex-support-frontend/` (UI pages: login, permission import, user-to-group import, create user csv)
- `samples/Import_user_to_group.xlsx` (ไฟล์ตัวอย่าง)
- `docker-compose.yml` (compose เฉพาะ beflex-support)
- `proxy/nginx.conf` (optional local proxy สำหรับทดสอบ URL `/support`)

## Quick start
1. คัดลอก `.env.example` เป็น `.env`
2. กรอกค่า Extension DB + Alfresco + credential manager
3. รัน:
   docker compose up -d
4. เปิดใช้งาน: http://localhost:8088/support/

หมายเหตุสำคัญของ compose (sync กับ `/opt/beflex-workspace`):
- ใช้ image tag (`BEFLEX_SUPPORT_BACKEND_TAG`, `BEFLEX_SUPPORT_FRONTEND_TAG`) แทนการ build ใน compose
- `beflex-support-frontend` ไม่ bind mount ไฟล์หน้าเว็บจาก host แล้ว (เสิร์ฟไฟล์จาก image)
- backend mount เฉพาะ upload path: `${DATA_VOLUME}/support-uploads:/app/uploads`

ถ้าต้องการ build image ใหม่ก่อนรัน compose:
```bash
docker build -t reg.bcecm.com/support/beflex-support-backend:0.0.3 ./beflex-support-backend
docker build -t reg.bcecm.com/support/beflex-support-frontend:0.0.3 ./beflex-support-frontend
```

หมายเหตุ frontend route:
- หน้า frontend ของ beflex-support ถูกเสิร์ฟที่ path `/support/` ดังนั้น `index.html` ต้องใช้ `<base href="/support/">`

## Available pages

- `index.html` : Login
- `service.html` : Import Permission (.xlsx)
- `group-service.html` : Import User to Group (.xlsx)
- `user-csv-service.html` : Create/Update User from CSV (.csv)
- `pm.html` : PM report (server/alfresco detail by script), schedule, retention, manual run
- `audit.html` : Audit report (service summary + event log)
- `query-sizing.html` : Query Search API เพื่อสรุปจำนวนไฟล์และขนาดรวม
- `support-other.html` : รวมลิงก์บริการที่เกี่ยวข้อง

### Page behavior (ล่าสุด)

- `audit.html`
   - `Audit Service Summary` หุบ/ขยายได้ (default: หุบ)
   - `Audit Events` หุบ/ขยายได้ (default: หุบ)
   - `Audit Events` มี paging `10 / 30 / 100` พร้อม `Prev/Next`
- `query-sizing.html`
   - `Run Query` หุบ/ขยายได้ (default: หุบ)
   - `Query Report` รองรับเลือกหลายรายการข้ามหน้า
   - ปุ่มใน `Query Report`: `Check all`, `Uncheck page`, `Clear selected`, `Export selected CSV`, `Export all CSV`

## Login policy

- beflex-support login ต้องเป็นสมาชิกกลุ่ม Alfresco: `GROUP_SUPPORT_WORKSPCE` (ชื่อกลุ่ม `SUPPORT_WORKSPCE`)

## API endpoints (main)

- `POST /api/beflex-support/imports` : Permission import (.xlsx)
- `POST /api/beflex-support/group-memberships/import` : User to Group import (.xlsx)
- `POST /api/beflex-support/users/import-csv` : Create/Update user import (.csv)
- `GET /api/beflex-support/pm/config` : Load PM config
- `PUT /api/beflex-support/pm/config` : Update PM config
- `POST /api/beflex-support/pm/run` : Trigger PM run manually
- `GET /api/beflex-support/pm/runs` : PM run history (`errors_only=true` for error tier)
- `GET /api/beflex-support/tasks/:id` : task status
- `GET /api/beflex-support/tasks/:id/logs` : task logs
- `POST /api/beflex-support/query-sizing/runs` : เริ่มงาน query sizing (async)
- `GET /api/beflex-support/query-sizing/runs/:id` : ดูสถานะงาน query sizing
- `GET /api/beflex-support/reports/query-sizing` : report query sizing (paging 10/30/100)
- `DELETE /api/beflex-support/reports/query-sizing/:id` : ลบ report รายการเดียว
- `GET /api/beflex-support/reports/query-sizing/export.csv` : export query sizing ทั้งหมด (CSV)
- `POST /api/beflex-support/reports/query-sizing/export.csv` : export query sizing ตามรายการที่เลือก (`ids[]`)

Query sizing notes:
- backend fix paging เป็น `maxItems=100` ต่อรอบเพื่อคุมโหลด
- ถ้า query ที่กรอกมาเป็นรูปแบบ JSON-escaped (`\\"`) ระบบจะ normalize อัตโนมัติเป็น `"` ก่อนยิง Alfresco API
- หน้า frontend แสดง warning เมื่อ query ดูเป็น JSON-escaped

PM feature summary:
- ใช้ script `pm_bc.sh` เพื่อเก็บข้อมูล server/alfresco report
- รองรับ cron schedule (crontab expression)
- รองรับ manual run จาก UI
- รองรับ retention ลบข้อมูล report/run history ตามจำนวนวันที่กำหนด
- UI แก้ค่าเฉพาะ: `customer`, `environment`, `cron`, `retentionDays`
- ค่า `.env` สำหรับ PM script อ้างอิงจาก volume: `./.env:/app/alfresco/.env:ro`

PM/contentstore note:
- หาก backend ต้องอ่าน contentstore โดยตรง (เช่น PM script) ต้อง mount host volume เข้า `beflex-support-backend`
- ตัวอย่าง mount: `${DATA_VOLUME}/alf-repo-data/contentstore:/mnt/alfresco/contentstore:ro`

PM host-data mounts (recommended):
- `${DATA_VOLUME}/alf-repo-data/contentstore:/mnt/alfresco/contentstore:ro`
- `${DATA_VOLUME}/postgresql-data:/mnt/alfresco/postgresql-data:ro`
- `${DATA_VOLUME}/solr-data:/mnt/alfresco/solr-data:ro`
- `./.env:/app/alfresco/.env:ro`

PM environment variables for backend:
- `PM_CONTENT_PATH=/mnt/alfresco/contentstore`
- `PM_POSTGRES_PATH=/mnt/alfresco/postgresql-data`
- `PM_SOLR_PATH=/mnt/alfresco/solr-data`
- `PM_ENV_WORKSPACE=/app/alfresco/.env`
- `PM_ENV_POSTGRESQL=/app/alfresco/.env`

## Alfresco authentication policy

- ทุก service import ของ beflex-support (`permission-import`, `group-member-import`, `user-csv-import`) จะตรวจสอบการ authen กับ Alfresco ผ่าน credential-manager ก่อนเริ่มประมวลผล
- backend ใช้ credential จาก credential-manager เท่านั้นสำหรับ service account (ไม่มี fallback ไป env credential)
- สำหรับงานที่จัดการ user/group ระบบจะตรวจ required group (`GROUP_ALFRESCO_ADMINISTRATORS` โดย default)

### Central auth provider (for future services)

- backend แยก service กลางสำหรับ auth ไว้ที่ `beflex-support-backend/services/alfresco-auth-provider.js`
- service ใหม่ควรเรียกผ่าน provider กลาง (`getValidatedServiceAuth`) แทนการยิง credential-manager โดยตรงใน business flow
- benefit: ลดโค้ดซ้ำ, ปรับนโยบาย auth ครั้งเดียวมีผลกับทุก service, และบังคับมาตรฐาน log/error เดียวกัน

หมายเหตุของ `POST /api/beflex-support/users/import-csv`:
- ระบบจะพยายามสร้าง user ด้วย Public API ก่อน (`/alfresco/api/-default-/public/alfresco/versions/1/people`)
- หากเจอปัญหา compatibility บาง environment จะ fallback ไป Legacy Share API (`/alfresco/s/api/people` หรือ `/alfresco/service/api/people`)

## Frontend session timeout

- หน้า `service.html`, `group-service.html`, `user-csv-service.html` มี idle session timeout ฝั่ง frontend
- ค่า default: 30 นาที (ไม่มี activity เช่น click/keydown/scroll/touch/focus)
- เมื่อ timeout จะ `logout` อัตโนมัติ กลับหน้า `index.html` และแสดงข้อความให้ login ใหม่
- สามารถปรับค่าได้ผ่าน attribute `data-session-timeout-minutes` ในแต่ละหน้า

## Scripts

### `sync-and-push.sh`
สคริปต์นี้ใช้สำหรับ sync โค้ดล่าสุดจาก workspace หลักไปยัง repo export นี้ แล้ว commit/push อัตโนมัติ

#### สิ่งที่สคริปต์ทำ (ตามลำดับ)
1. `rsync` โฟลเดอร์ `beflex-support-backend/` จาก source → export repo
2. `rsync` โฟลเดอร์ `beflex-support-frontend/` จาก source → export repo
3. แสดง `git status --short`
4. ถ้ามีการเปลี่ยนแปลง: `git add` + `git commit`
5. `git push` ไป branch ที่กำหนด

#### Default paths / branch
- `SRC_ROOT=/opt/beflex-workspace`
- `DST_ROOT=/opt/beflex-support/repo`
- `BRANCH=main`

#### การใช้งาน
```bash
cd /opt/beflex-support/repo
./sync-and-push.sh
```

กำหนด commit message เอง:
```bash
./sync-and-push.sh "chore: sync beflex-support release 0.0.2"
```

กำหนด path/branch ชั่วคราวตอนรัน:
```bash
SRC_ROOT=/opt/beflex-workspace \
DST_ROOT=/opt/beflex-support/repo \
BRANCH=main \
./sync-and-push.sh "chore: sync latest from workspace"
```

#### หมายเหตุ
- ถ้าไม่มี diff สคริปต์จะจบทันทีด้วยข้อความ `No changes to commit.`
- ต้องมีสิทธิ์ push ไปยัง remote (`origin`) ของ branch เป้าหมาย
- สคริปต์ใช้ `set -euo pipefail` ถ้าขั้นตอนไหนล้มเหลวจะหยุดทันที

## Maintenance Rule

- หากมีการปรับแก้ส่วน **beflex-support** (backend/frontend/config/route/page/menu) ต้องอัปเดต `README.md` ไฟล์นี้ทุกครั้งก่อน push
- การอัปเดต README ควรครอบคลุมอย่างน้อย: สิ่งที่เปลี่ยน, วิธีใช้งาน, endpoint/page ใหม่, และผลกระทบที่ผู้ใช้งานควรรู้
