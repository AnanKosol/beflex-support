# allops-raku

Repository นี้คัดเฉพาะส่วน **AllOps-Raku service** เท่านั้น (ไม่มีไฟล์ระบบอื่นจาก beflex-workspace)

## Included
- `allops-raku-backend/` (Node.js API)
- `allops-raku-frontend/` (UI pages: login, permission import, user-to-group import, create user csv)
- `samples/Import_user_to_group.xlsx` (ไฟล์ตัวอย่าง)
- `docker-compose.yml` (compose เฉพาะ allops-raku)
- `proxy/nginx.conf` (optional local proxy สำหรับทดสอบ URL `/raku`)

## Quick start
1. คัดลอก `.env.example` เป็น `.env`
2. กรอกค่า Extension DB + Alfresco + credential manager
3. รัน:
   docker compose up -d --build
4. เปิดใช้งาน: http://localhost:8088/raku/

## Available pages

- `index.html` : Login
- `service.html` : Import Permission (.xlsx)
- `group-service.html` : Import User to Group (.xlsx)
- `user-csv-service.html` : Create/Update User from CSV (.csv)
- `support-other.html` : รวมลิงก์บริการที่เกี่ยวข้อง

## API endpoints (main)

- `POST /api/allops-raku/imports` : Permission import (.xlsx)
- `POST /api/allops-raku/group-memberships/import` : User to Group import (.xlsx)
- `POST /api/allops-raku/users/import-csv` : Create/Update user import (.csv)
- `GET /api/allops-raku/tasks/:id` : task status
- `GET /api/allops-raku/tasks/:id/logs` : task logs

## Alfresco authentication policy

- ทุก service import ของ Raku (`permission-import`, `group-member-import`, `user-csv-import`) จะตรวจสอบการ authen กับ Alfresco ผ่าน credential-manager ก่อนเริ่มประมวลผล
- backend ใช้ credential จาก credential-manager เท่านั้นสำหรับ service account (ไม่มี fallback ไป env credential)
- สำหรับงานที่จัดการ user/group ระบบจะตรวจ required group (`GROUP_ALFRESCO_ADMINISTRATORS` โดย default)

### Central auth provider (for future services)

- backend แยก service กลางสำหรับ auth ไว้ที่ `allops-raku-backend/services/alfresco-auth-provider.js`
- service ใหม่ควรเรียกผ่าน provider กลาง (`getValidatedServiceAuth`) แทนการยิง credential-manager โดยตรงใน business flow
- benefit: ลดโค้ดซ้ำ, ปรับนโยบาย auth ครั้งเดียวมีผลกับทุก service, และบังคับมาตรฐาน log/error เดียวกัน

หมายเหตุของ `POST /api/allops-raku/users/import-csv`:
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
1. `rsync` โฟลเดอร์ `allops-raku-backend/` จาก source → export repo
2. `rsync` โฟลเดอร์ `allops-raku-frontend/` จาก source → export repo
3. แสดง `git status --short`
4. ถ้ามีการเปลี่ยนแปลง: `git add` + `git commit`
5. `git push` ไป branch ที่กำหนด

#### Default paths / branch
- `SRC_ROOT=/opt/beflex-workspace`
- `DST_ROOT=/opt/allops-raku-export/repo`
- `BRANCH=main`

#### การใช้งาน
```bash
cd /opt/allops-raku-export/repo
./sync-and-push.sh
```

กำหนด commit message เอง:
```bash
./sync-and-push.sh "chore: sync raku release 0.0.2"
```

กำหนด path/branch ชั่วคราวตอนรัน:
```bash
SRC_ROOT=/opt/beflex-workspace \
DST_ROOT=/opt/allops-raku-export/repo \
BRANCH=main \
./sync-and-push.sh "chore: sync latest from workspace"
```

#### หมายเหตุ
- ถ้าไม่มี diff สคริปต์จะจบทันทีด้วยข้อความ `No changes to commit.`
- ต้องมีสิทธิ์ push ไปยัง remote (`origin`) ของ branch เป้าหมาย
- สคริปต์ใช้ `set -euo pipefail` ถ้าขั้นตอนไหนล้มเหลวจะหยุดทันที

## Maintenance Rule

- หากมีการปรับแก้ส่วน **AllOps-Raku** (backend/frontend/config/route/page/menu) ต้องอัปเดต `README.md` ไฟล์นี้ทุกครั้งก่อน push
- การอัปเดต README ควรครอบคลุมอย่างน้อย: สิ่งที่เปลี่ยน, วิธีใช้งาน, endpoint/page ใหม่, และผลกระทบที่ผู้ใช้งานควรรู้
