# beflex-support-backend

Backend service for beflex-support permission Excel import.

## Features
- Login against Alfresco ticket API
- Group membership check (`GROUP_SUPPORT_WORKSPCE`)
- Issues support JWT for frontend
- Accepts `.xlsx` upload and creates async task
- Calls `permission-service` in background with retry
- Supports group-member import from Excel (`group_id`, `group_display_name`, `user_id`, `action`)
- Audit trail in PostgreSQL Extension DB (`timestamp`, `username`, `action_type`, `filename`, `status`)
- Task logs and import report APIs
- Centralized audit events per service (`service_name`, `action_type`, `status`, `metadata`)

## Database
- Uses PostgreSQL from environment: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- Uses shared tables for all beflex-support services (no new table per service):
	- `allops_raku_imports`
	- `allops_raku_task_logs`
	- `allops_raku_audit_events`

## Audit APIs
- `GET /api/reports/audit?service_name=permission-import&status=FAILED&username=<user>&limit=100`
- `GET /api/reports/audit/services`

## Credential Manager integration
- Supports sample API patterns:
	- `GET /credentials/1/value` and `GET /credentials/2/value`
	- `GET /export/service/alfresco`
- Also supports proxied patterns under `/api/credential-manager/...`

## APIs
- `POST /api/auth/login`
- `POST /api/imports` (Bearer token)
- `POST /api/group-memberships/import` (Bearer token)
- `GET /api/tasks/:id` (Bearer token)
- `GET /api/tasks/:id/logs` (Bearer token)
- `GET /api/reports/imports?service_name=permission-import|group-member-import` (Bearer token)
- `GET /health`
