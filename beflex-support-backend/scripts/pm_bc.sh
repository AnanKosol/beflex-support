#!/usr/bin/env bash
#
# Server sizing report helper.

set -uo pipefail

# --- Configuration ---------------------------------------------------------
# --- CUSTOMER DETAILS ---------------------------------------------------------
CUSTOMER=${CUSTOMER:-"aegis"}
ENVIRONMENT=${ENVIRONMENT:-"Prod"}
CONTENT_PATH=${CONTENT_PATH:-"/opt/data/office/alf-repo-data/contentstore"}
POSTGRES_PATH=${POSTGRES_PATH:-"/opt/data/office/postgresql-data"}
SOLR_PATH=${SOLR_PATH:-"/opt/data/office/solr-data"}
OUTPUT_DIR=${1:-${OUTPUT_DIR:-"/opt/00_sup/script"}}
IP_INCLUDE_REGEX=${IP_INCLUDE_REGEX:-'^(en|eth|eno|enp|ens|em|bond|team|wlan)'}
# --- ENV BACKUP ---------------------------------------------------------
ENV_WORKSPACE=${ENV_WORKSPACE:-"/opt/beflex-workspace/.env"}
ENV_POSTGRESQL=${ENV_POSTGRESQL:-"/opt/beflex-workspace/.env"}
WORKSPACE_SOURCE_DIR=${WORKSPACE_SOURCE_DIR:-"/opt/beflex-workspace"}
POSTGRES_SOURCE_DIR=${POSTGRES_SOURCE_DIR:-"/opt/beflex-db"}
BACKUP_DIR=${BACKUP_DIR:-"/opt/00_sup/script/backup"}
WORKSPACE_BACKUP_EXCLUDES=${WORKSPACE_BACKUP_EXCLUDES:-"backup data logs config/glowroot"}
POSTGRES_BACKUP_EXCLUDES=${POSTGRES_BACKUP_EXCLUDES:-""}
# --- System setting ---------------------------------------------------------
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S %Z')
FILE_TIMESTAMP=$(date '+%Y-%m-%d_%H-%M')
BACKUP_DATE=$(date '+%Y-%m-%d')
SEPARATOR_LINE=${SEPARATOR_LINE:-"====================================================================="}
# --- CODE ---------------------------------------------------------
sanitize_token() {
	local token=$1
	token=${token// /_}
	token=$(printf '%s' "$token" | tr -cd '[:alnum:]_-')
	if [[ -z $token ]]; then
		token="unknown"
	fi
	echo "$token"
}

CUSTOMER_TOKEN=$(sanitize_token "$CUSTOMER")
ENVIRONMENT_TOKEN=$(sanitize_token "$ENVIRONMENT")

FILENAME="pm_${CUSTOMER_TOKEN}_${ENVIRONMENT_TOKEN}_${FILE_TIMESTAMP}.txt"
if [[ $OUTPUT_DIR != "/" ]]; then
	OUTPUT_DIR=${OUTPUT_DIR%/}
fi
mkdir -p "$OUTPUT_DIR"
OUTPUT_FILE="$OUTPUT_DIR/$FILENAME"

# --- Helpers ----------------------------------------------------------------
print_block() {
	local title=$1
	shift
	{
		echo "$title"
		printf '%*s\n' "${#title}" '' | tr ' ' '-'
		if ! "$@"; then
			echo "[command failed]"
		fi
		echo
	} >>"$OUTPUT_FILE" 2>&1
}

print_literal() {
	{
		echo "$1"
		printf '%*s\n' "${#1}" '' | tr ' ' '-'
		printf '%s\n\n' "$2"
	} >>"$OUTPUT_FILE"
}

print_banner() {
	local title=$1
	{
		echo "$SEPARATOR_LINE"
		printf '#%s\n' "$title"
		echo "$SEPARATOR_LINE"
	} >>"$OUTPUT_FILE"
}

safe_du() {
	local path=$1
	if [[ -d $path ]]; then
		du -sh -- "$path"
	else
		echo "$path does not exist"
		return 1
	fi
}

command_exists() {
	command -v "$1" >/dev/null 2>&1
}

mask_env_value() {
	local key=$1
	local value=$2
	case $key in
		PGPASSWORD|PGPASSWORD_FILE)
			printf '********'
			;;
		*)
			printf '%s' "$value"
			;;
	esac
}

extract_env_value() {
	local file=$1
	local key=$2
	[[ -f $file ]] || return 1
	awk -v key="$key" '
		/^[[:space:]]*#/ {next}
		{
			line = $0
			gsub(/\r/, "", line)
			if (line ~ "^[[:space:]]*" key "[[:space:]]*=") {
				sub("^[[:space:]]*" key "[[:space:]]*=[[:space:]]*", "", line)
				sub(/[[:space:]]+#.*$/, "", line)
				sub(/^[[:space:]]+/, "", line)
				sub(/[[:space:]]+$/, "", line)
				print line
				exit
			}
		}
	' "$file"
}

print_env_details_section() {
	local title=$1
	local file=$2
	shift 2
	local keys=("$@")
	print_banner "$title"
	if [[ ! -f $file ]]; then
		printf '%s not found\n\n' "$file" >>"$OUTPUT_FILE"
		return 1
	fi
	local found=0
	for key in "${keys[@]}"; do
		local value
		value=$(extract_env_value "$file" "$key") || true
		if [[ -n $value ]]; then
			printf '%s: %s\n' "$key" "$(mask_env_value "$key" "$value")" >>"$OUTPUT_FILE"
			found=1
		fi
	done
	if [[ $found -eq 0 ]]; then
		printf 'No matching keys found in %s\n' "$file" >>"$OUTPUT_FILE"
	fi
	echo >>"$OUTPUT_FILE"
}

print_env_image_versions_section() {
	local title=$1
	local file=$2
	print_banner "$title"
	if [[ ! -f $file ]]; then
		printf '%s not found\n\n' "$file" >>"$OUTPUT_FILE"
		return 1
	fi

	local found=0
	while IFS= read -r key; do
		[[ -n $key ]] || continue
		local value
		value=$(extract_env_value "$file" "$key") || true
		if [[ -n $value ]]; then
			printf '%s: %s\n' "$key" "$(mask_env_value "$key" "$value")" >>"$OUTPUT_FILE"
			found=1
		fi
	done < <(
		awk '
			/^[[:space:]]*#/ {next}
			/^[[:space:]]*$/ {next}
			{
				line = $0
				sub(/^[[:space:]]+/, "", line)
				if (line !~ /^[A-Z0-9_]+[[:space:]]*=/) next
				key = line
				sub(/[[:space:]]*=.*$/, "", key)
				if (key == "SERVER_NAME") exit
				print key
			}
		' "$file"
	)

	if [[ $found -eq 0 ]]; then
		printf 'No matching keys found in %s\n' "$file" >>"$OUTPUT_FILE"
	fi
	echo >>"$OUTPUT_FILE"
}

safe_find_counts() {
	local base=$1
	if [[ -d $base ]]; then
		find "$base" -mindepth 1 -maxdepth 1 -type d -exec sh -c 'echo -n "$1: "; find "$1" -type f | wc -l' _ {} \; | sort -k2 -n
	else
		echo "$base does not exist"
		return 1
	fi
}

run_backup_job() {
	local label=$1
	local source_dir=$2
	local dest_dir=$3
	local excludes=$4
	if [[ ! -d $source_dir ]]; then
		printf '%s backup: source %s not found\n' "$label" "$source_dir"
		return 1
	fi
	if ! command_exists zip; then
		printf '%s backup: zip command not found\n' "$label"
		return 1
	fi
	mkdir -p "$dest_dir"
	local zip_path="$dest_dir/pm_${label}_${BACKUP_DATE}.zip"
	rm -f "$zip_path"
	local -a zip_cmd=(zip -r "$zip_path" .)
	local -a exclude_array=()
	if [[ -n $excludes ]]; then
		IFS=' ' read -r -a exclude_array <<<"$excludes"
	fi
	for pattern in "${exclude_array[@]}"; do
		[[ -z $pattern ]] && continue
		pattern=${pattern#/}
		zip_cmd+=(-x "./$pattern" "./$pattern/*")
	done
	pushd "$source_dir" >/dev/null 2>&1 || {
		printf '%s backup: cannot access %s\n' "$label" "$source_dir"
		return 1
	}
	if ! "${zip_cmd[@]}" >/dev/null 2>&1; then
		popd >/dev/null 2>&1
		printf '%s backup: zip command failed\n' "$label"
		return 1
	fi
	popd >/dev/null 2>&1
	printf '%s backup file: %s\n' "$label" "$zip_path"
}

format_directory_size() {
	local path=$1
	local output
	output=$(safe_du "$path")
	local status=$?
	if [[ $status -eq 0 ]]; then
		printf '%s' "$output" | awk '{print $1}'
	else
		printf '%s' "$output"
	fi
}

print_contentstore_file_totals() {
	local base=$1
	if [[ ! -d $base ]]; then
		printf '%s does not exist\n\n' "$base" >>"$OUTPUT_FILE"
		return 1
	fi
	local total=0
	while IFS=: read -r dir count; do
		count=$(printf '%s' "$count" | tr -d '[:space:]')
		printf '%s: %s\n' "$dir" "$count" >>"$OUTPUT_FILE"
		if [[ $count =~ ^[0-9]+$ ]]; then
			total=$((total + count))
		fi
	done < <(
		find "$base" -mindepth 1 -maxdepth 1 -type d \
			-exec bash -c 'dir="$1"; count=$(find "$1" -type f | wc -l | awk "{print \$1}"); printf "%s:%s\n" "$dir" "$count"' _ {} \; \
		| sort -t: -k2,2n
	)
	printf 'Total file: %s file\n\n' "$total" >>"$OUTPUT_FILE"
}

print_contentstore_year_summary() {
	local base=$1
	if [[ -d $base ]]; then
		while IFS=$'\t' read -r size path; do
			[[ -z $size ]] && continue
			if [[ $path == "$base" ]]; then
				continue
			fi
			printf '%s\t%s\n' "$size" "$path" >>"$OUTPUT_FILE"
		done < <(du -h --max-depth=1 -- "$base" | sort -k2V)
		echo >>"$OUTPUT_FILE"
	else
		printf '%s does not exist\n\n' "$base" >>"$OUTPUT_FILE"
		return 1
	fi
}

print_sizing_banner() {
	local title=$1
	local path=$2
	print_banner "$title"
	safe_du "$path" >>"$OUTPUT_FILE" 2>&1
	echo >>"$OUTPUT_FILE"
}

print_last_months() {
	local base=$1
	local months=${2:-12}
	{
		local start_ref
		start_ref=$(date +%Y-%m-01)
		for offset in $(seq 0 $((months - 1))); do
			local target
			target=$(date -d "$start_ref -$offset month" +%Y-%m)
			local year=${target%-*}
			local month=${target#*-}
			local month_display=$((10#$month))
			local dir_padded="$base/$year/$month"
			local dir_plain="$base/$year/$month_display"
			local dir=""
			if [[ -d $dir_padded ]]; then
				dir=$dir_padded
			elif [[ -d $dir_plain ]]; then
				dir=$dir_plain
			fi
			local size
			if [[ -n $dir ]]; then
				size=$(du -sh -- "$dir" 2>/dev/null | awk '{print $1}')
			else
				size="missing"
			fi
			printf '%s/%s: %s\n' "$year" "$month_display" "$size"
		done
		echo
	} >>"$OUTPUT_FILE"
}

print_backup_section() {
	print_banner "Back up"
	{
		run_backup_job "workspace" "$WORKSPACE_SOURCE_DIR" "$BACKUP_DIR" "$WORKSPACE_BACKUP_EXCLUDES"
		run_backup_job "postgresql" "$POSTGRES_SOURCE_DIR" "$BACKUP_DIR" "$POSTGRES_BACKUP_EXCLUDES"
		echo
	} >>"$OUTPUT_FILE"
}

print_docker_details() {
	print_banner "Docker details"
	{
		local docker_version="docker command not found"
		local docker_compose_version="docker compose command not found"
		if command_exists docker; then
			docker_version=$(docker --version 2>&1 | head -n 1)
			local compose_output
			compose_output=$(docker compose version 2>&1 | head -n 1)
			if [[ -n $compose_output ]]; then
				docker_compose_version=$compose_output
			fi
		fi
		if [[ $docker_compose_version == "docker compose command not found" ]] && command_exists docker-compose; then
			docker_compose_version=$(docker-compose --version 2>&1 | head -n 1)
		fi
		echo "docker v: $docker_version"
		echo "docker compose v: $docker_compose_version"
		echo "docker stats:"
		if command_exists docker; then
			local stats_output
			local stats_header=$'CONTAINER ID	NAME	CPU %	MEM USAGE / LIMIT	MEM %	NET I/O	BLOCK I/O	PIDS'
			local stats_cmd=(docker stats --no-stream --format "{{.Container}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}")
			if command_exists timeout; then
				stats_output=$(timeout 5s "${stats_cmd[@]}" 2>&1)
			else
				stats_output=$("${stats_cmd[@]}" 2>&1)
			fi
			if [[ -n $stats_output ]]; then
				echo "$stats_header"
				printf '%s\n' "$stats_output"
			else
				echo "[docker stats failed]"
			fi
		else
			echo "docker command not found"
		fi
		echo
	} >>"$OUTPUT_FILE"
}

# --- Derived values ---------------------------------------------------------
HOSTNAME_VALUE=$(hostname)
MEM_TOTAL_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
MEM_AVAILABLE_KB=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
MEM_USED_KB=$((MEM_TOTAL_KB - MEM_AVAILABLE_KB))
RAM_TOTAL_GB=$(awk -v total="$MEM_TOTAL_KB" 'BEGIN {printf "%.2f", total/1048576}')
RAM_USED_GB=$(awk -v used="$MEM_USED_KB" 'BEGIN {printf "%.2f", used/1048576}')
CPU_COUNT=$(grep -c '^processor' /proc/cpuinfo)
CPU_MODEL=$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2}' /proc/cpuinfo | sort -u | paste -sd ', ' -)
CPU_MODEL=${CPU_MODEL:-"Unknown"}
OS_RELEASE=$(bash -c 'cat /etc/*release 2>/dev/null | grep "DISTRIB_DESCRIPTION" | sort -u | paste -sd ", " -')
OS_RELEASE=${OS_RELEASE:-"Unknown"}
PRIMARY_IPS=$(ip -o -4 addr show 2>/dev/null | awk -v pattern="$IP_INCLUDE_REGEX" '($2 ~ pattern) {split($4, a, "/"); print a[1] " (" $2 ")"}' | paste -sd ', ' - || true)
PRIMARY_IPS=${PRIMARY_IPS:-"Unknown"}
DISK_REPORT=$(df -h | grep -v '/docker/overlay2')

# --- Output -----------------------------------------------------------------
# Server Details
cat <<REPORT >"$OUTPUT_FILE"
-------------------------------------------------
# Server Details
-------------------------------------------------
Customer: $CUSTOMER
Environment: $ENVIRONMENT
Generated At: $TIMESTAMP

Hostname: $HOSTNAME_VALUE
RAM Total: ${RAM_TOTAL_GB} GB
RAM Used: ${RAM_USED_GB} GB
CPU Processor: $CPU_COUNT
CPU Model Name: $CPU_MODEL
OS Release: $OS_RELEASE
ip addr show: $PRIMARY_IPS

Harddisk Server:
$DISK_REPORT

REPORT

print_env_image_versions_section "beflex app details" "$ENV_WORKSPACE"

print_env_details_section "postgresql details" "$ENV_POSTGRESQL" \
	"POSTGRES_TAG"

# Alfresco Content store
print_banner "Alfresco Content Store"
{
	printf 'Alf ContentStore path=%s\n' "$CONTENT_PATH"
	printf 'Alf ContentStore sizing= %s\n' "$(format_directory_size "$CONTENT_PATH")"
	echo
} >>"$OUTPUT_FILE"

print_banner "Alf ContentStore year"
print_contentstore_year_summary "$CONTENT_PATH"

print_banner "Alf ContentStore 13M older"
print_last_months "$CONTENT_PATH" 13

print_banner "Alf Contnetstore total file"
print_contentstore_file_totals "$CONTENT_PATH"

# Database sizing
print_sizing_banner "Database sizing (PostgreSQL)" "$POSTGRES_PATH"

# Solr sizing
print_sizing_banner "Solr sizing" "$SOLR_PATH/solr-data"

print_backup_section

print_docker_details

echo "Report written to $OUTPUT_FILE"