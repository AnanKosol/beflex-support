# PM Page Guide (เข้าใจง่าย)

## เป้าหมายของหน้า PM
หน้า PM ใช้สำหรับ 3 งานหลัก:
1. จัดเก็บโครงสร้างระบบ (Registry)
2. ส่งงานให้ PM Agent (Dispatch)
3. ติดตามสถานะงาน (Jobs / Runs)

---

## โครงสร้างการใช้งาน (แนะนำ)

### Step 1: PM Registry
กรอกข้อมูลโครงสร้างหลัก:
- Customer Code
- Environment
- Server Key / Server Name / Server Host
- Site Code
- Applications JSON Array

จากนั้นกด **Save Registry**

> หมายเหตุ: ตอนนี้ใช้ **Customer Code** เท่านั้น (ไม่ใช้ Customer Name)

---

### Step 2: Registry Preview
ตรวจว่ารายการที่บันทึกขึ้นในตารางถูกต้อง:
- Customer
- Environment
- Server
- Application
- Service

---

### Step 3: Dispatch PM Job
เลือก:
- Server
- Application (หรือ ALL)
- Trigger (MANUAL/SCHEDULE/CRON)

แล้วกด **Dispatch Job**

---

### Step 4: PM Agent Jobs
ติดตามงานที่ถูกส่งไปแล้ว:
- สถานะคิวงาน
- Agent ที่รับงาน

---

### Step 5: PM Runs
ดูผลการรันย้อนหลัง และเปิด **Show errors only** เมื่อต้องการโฟกัสเฉพาะงานผิดพลาด

---

## Applications JSON ตัวอย่างขั้นต่ำ

```json
[
  {
    "appType": "beflex-workspace",
    "appName": "workspace"
  }
]
```

## ปุ่มช่วยเตรียม JSON
- Load Joget / PostgreSQL / beflex-workspace / Other
- Load All Templates
- Pretty Format JSON
- Copy Min Schema
- Clear Applications JSON
- Validate JSON Schema

---

## Advanced: PM Settings (ใช้เมื่อจำเป็น)
ใช้ตั้งค่า cron, retention และสั่ง Run Manual ทันที
- Save Settings
- Run Manual Now
- Refresh

หากเป็นการใช้งานทั่วไปประจำวัน ให้เริ่มจาก Registry → Dispatch → Jobs/Runs ก่อน
