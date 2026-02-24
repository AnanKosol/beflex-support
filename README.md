# allops-raku

Repository นี้คัดเฉพาะส่วน **AllOps-Raku service** เท่านั้น (ไม่มีไฟล์ระบบอื่นจาก beflex-workspace)

## Included
- `allops-raku-backend/` (Node.js API)
- `allops-raku-frontend/` (UI pages: login, permission import, user-to-group import)
- `samples/Import_user_to_group.xlsx` (ไฟล์ตัวอย่าง)
- `docker-compose.yml` (compose เฉพาะ allops-raku)
- `proxy/nginx.conf` (optional local proxy สำหรับทดสอบ URL `/raku`)

## Quick start
1. คัดลอก `.env.example` เป็น `.env`
2. กรอกค่า Extension DB + Alfresco + credential manager
3. รัน:
   docker compose up -d --build
4. เปิดใช้งาน: http://localhost:8088/raku/
