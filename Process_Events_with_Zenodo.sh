#!/bin/bash

# Process_Events_with_Zenodo.sh
# Main script to process CASES events and upload to Zenodo
# Usage: ./Process_Events_with_Zenodo.sh [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description] [bin_type] [staging_dir] [max_archive_size_gb] [grid_filter] [max_zenodo_deposition_gb]
#
# Parameters:
#   events_file           - Input file with event list (default: "List_of_events.txt")
#   firmware_version      - Firmware version for binflate (default: "gss1400")
#   zenodo_token          - Zenodo API access token (required)
#   zenodo_title          - Title for Zenodo deposition
#   zenodo_description    - Description for Zenodo deposition
#   bin_type              - File type: "dataout", "dataoutiq", or "both" (default: "both")
#   staging_dir           - Working directory for staging files
#   max_archive_size_gb   - Max size per archive file (default: 45, recommend 2-5 for reliable uploads)
#   grid_filter           - Receiver filter: specific grid name or "all" (default: "all")
#   max_zenodo_deposition_gb - Max size per Zenodo deposition before creating new part (default: 49, max 50)

EVENTS_FILE=${1:-"List_of_events.txt"}
FIRMWARE_VERSION=${2:-"gss1400"}
ZENODO_TOKEN=${3}
ZENODO_TITLE=${4:-"CASES Scintillation Event Data"}
ZENODO_DESCRIPTION=${5:-"Processed CASES scintillation event data with IQ, ionospheric, scintillation, navigation, channel, and transmitter information."}
BIN_TYPE=${6:-"both"}  # Options: "dataout", "dataoutiq", "both"
STAGING_DIR=${7:-"./staging_$(date +%Y%m%d_%H%M%S)"}
MAX_ARCHIVE_SIZE_GB=${8:-2}  # Small archives for reliable uploads (large uploads timeout)
GRID_FILTER=${9:-"all"}  # Grid filter: specific grid name or "all"
MAX_ZENODO_DEPOSITION_GB=${10:-49}  # Max size per Zenodo deposition (under 50GB limit)

# Array of base paths to search for data
BASE_PATHS=(
    "/data1/public/Data/cases/pfrr"
    "/data2/from_usb"
    "/data2/from_usb2"
    "/data2/from_usb3"
    "/data2/from_usb4"
    "/data2/from_usb6/archive"
)

# Store the original directory where script was run
SCRIPT_START_DIR="$(pwd)"

echo "CASES Data Processing and Zenodo Upload"
echo "======================================"
echo "Events file: $EVENTS_FILE"
echo "Firmware: $FIRMWARE_VERSION"
echo "Bin file type: $BIN_TYPE"
echo "Working directory: $SCRIPT_START_DIR"
echo "Staging directory: $STAGING_DIR"
echo "Max archive size: ${MAX_ARCHIVE_SIZE_GB}GB"
echo "Max Zenodo deposition size: ${MAX_ZENODO_DEPOSITION_GB}GB"
echo "Grid filter: $GRID_FILTER"
echo "Zenodo token: ${ZENODO_TOKEN:+[PROVIDED]}${ZENODO_TOKEN:-[NOT PROVIDED]}"
echo "Zenodo title: $ZENODO_TITLE"
echo ""

# Multi-part deposition tracking
DEPOSITION_PART=1
ALL_DEPOSITION_IDS=()
MAX_ZENODO_DEPOSITION_BYTES=$((MAX_ZENODO_DEPOSITION_GB * 1024 * 1024 * 1024))

# Check required parameters
if [ -z "$ZENODO_TOKEN" ]; then
    echo "Error: Zenodo token is required"
    echo "Usage: $0 [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description] [bin_type] [staging_dir] [max_archive_size_gb] [grid_filter]"
    exit 1
fi

# Validate BIN_TYPE parameter
if [[ ! "$BIN_TYPE" =~ ^(dataout|dataoutiq|both)$ ]]; then
    echo "Error: Invalid bin_type '$BIN_TYPE'. Must be 'dataout', 'dataoutiq', or 'both'"
    echo "Usage: $0 [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description] [bin_type] [staging_dir] [max_archive_size_gb] [grid_filter]"
    exit 1
fi

# Check if events file exists
if [ ! -f "$EVENTS_FILE" ]; then
    echo "Error: Events file not found: $EVENTS_FILE"
    exit 1
fi

# Create staging directory
if [ ! -d "$STAGING_DIR" ]; then
    mkdir -p "$STAGING_DIR"
    if [ $? -ne 0 ]; then
        echo "Error: Could not create staging directory: $STAGING_DIR"
        exit 1
    fi
    echo "Created staging directory: $STAGING_DIR"
fi

# Count total events
TOTAL_EVENTS=$(wc -l < "$EVENTS_FILE")
echo "Total events to process: $TOTAL_EVENTS"
echo ""

# Default receivers
ALL_RECEIVERS=("grid108" "grid154" "grid160" "grid161" "grid162" "grid163")

# Set receivers based on grid filter
if [ "$GRID_FILTER" = "all" ]; then
    RECEIVERS=("${ALL_RECEIVERS[@]}")
    echo "Processing all receivers: ${RECEIVERS[*]}"
else
    # Check if the specified grid is valid
    valid_grid=false
    for grid in "${ALL_RECEIVERS[@]}"; do
        if [ "$grid" = "$GRID_FILTER" ]; then
            valid_grid=true
            break
        fi
    done

    if [ "$valid_grid" = false ]; then
        echo "Error: Invalid grid filter '$GRID_FILTER'"
        echo "Valid options: all, ${ALL_RECEIVERS[*]}"
        exit 1
    fi

    RECEIVERS=("$GRID_FILTER")
    echo "Processing only receiver: $GRID_FILTER"
fi
echo ""

# Create log file
LOG_FILE="processing_$(date +%Y%m%d_%H%M%S).log"
echo "Log file: $LOG_FILE"
echo ""

# Function to log messages
log_msg() {
    echo "$1" | tee -a "$LOG_FILE"
}

# Function to get directory size in GB
get_size_gb() {
    local dir="$1"
    if [ -d "$dir" ]; then
        du -sb "$dir" | awk '{printf "%.2f", $1/1024/1024/1024}'
    else
        echo "0"
    fi
}

# Function to count files in directory
count_files() {
    local dir="$1"
    if [ -d "$dir" ]; then
        find "$dir" -type f | wc -l
    else
        echo "0"
    fi
}

# Function to get current Zenodo deposition size in bytes
# Queries the API and sums all file sizes using basic tools
get_zenodo_deposition_size() {
    local dep_id="$1"
    local response
    local total_size=0

    response=$(curl -s \
        -H "Authorization: Bearer ${ZENODO_TOKEN}" \
        "https://zenodo.org/api/deposit/depositions/${dep_id}/files")

    # Extract all filesize values and sum them using grep and awk
    # Response format: [{"filesize": 12345, ...}, {"filesize": 67890, ...}]
    total_size=$(echo "$response" | grep -o '"filesize": *[0-9]*' | grep -o '[0-9]*' | awk '{sum+=$1} END {print sum+0}')

    echo "$total_size"
}

# Function to create a new part deposition when size limit is reached
# Returns 0 on success, 1 on failure
# Sets global DEPOSITION_ID and BUCKET_URL to the new deposition
create_new_part_deposition() {
    local part_num="$1"
    local part_title

    # Create title with part number
    if [ $part_num -eq 1 ]; then
        part_title="$ZENODO_TITLE"
    else
        part_title="$ZENODO_TITLE (Part $part_num)"
    fi

    log_msg ""
    log_msg "=== CREATING NEW ZENODO DEPOSITION (Part $part_num) ==="

    # Create empty deposition
    local curl_output=$(mktemp)
    local http_code=$(curl -s -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -X POST \
        "https://zenodo.org/api/deposit/depositions?access_token=${ZENODO_TOKEN}" \
        -d '{}' \
        -o "$curl_output")

    local response_body=$(cat "$curl_output")
    rm -f "$curl_output"

    if [ "$http_code" != "201" ]; then
        log_msg "Error: Failed to create new deposition (HTTP $http_code)"
        log_msg "Response: $response_body"
        return 1
    fi

    # Parse the response
    local new_dep_id=$(echo "$response_body" | grep -o '"id": *[0-9]*' | head -1 | grep -o '[0-9]*')
    local new_bucket_url=$(echo "$response_body" | grep -o '"bucket": *"[^"]*"' | cut -d'"' -f4)

    if [ -z "$new_dep_id" ] || [ -z "$new_bucket_url" ]; then
        log_msg "Error: Could not extract deposition ID or bucket URL"
        return 1
    fi

    # Build description with links to other parts
    local part_description="$ZENODO_DESCRIPTION"
    if [ ${#ALL_DEPOSITION_IDS[@]} -gt 0 ]; then
        part_description="$part_description\n\nThis is Part $part_num of a multi-part dataset. Other parts:\n"
        local i=1
        for prev_id in "${ALL_DEPOSITION_IDS[@]}"; do
            part_description="$part_description- Part $i: https://zenodo.org/deposit/$prev_id\n"
            i=$((i + 1))
        done
    fi

    # Update metadata with part title
    local metadata_json='{
        "metadata": {
            "title": "'"$part_title"'",
            "upload_type": "dataset",
            "description": "'"$(echo -e "$part_description")"'",
            "creators": [{"name": "CASES Team", "affiliation": "Research Institution"}],
            "keywords": ["CASES", "scintillation", "GPS", "ionosphere"]
        }
    }'

    local metadata_response=$(curl -s -w "%{http_code}\n" \
        -H "Content-Type: application/json" \
        -X PUT \
        "https://zenodo.org/api/deposit/depositions/${new_dep_id}?access_token=${ZENODO_TOKEN}" \
        -d "$metadata_json")

    local metadata_http_code=$(echo "$metadata_response" | tail -n 1)

    if [ "$metadata_http_code" != "200" ]; then
        log_msg "Warning: Failed to update metadata for part $part_num (HTTP $metadata_http_code)"
    fi

    # Update global variables
    DEPOSITION_ID="$new_dep_id"
    BUCKET_URL="$new_bucket_url"
    ALL_DEPOSITION_IDS+=("$new_dep_id")

    log_msg "Successfully created Part $part_num deposition"
    log_msg "  Deposition ID: $DEPOSITION_ID"
    log_msg "  Bucket URL: $BUCKET_URL"

    return 0
}

# Debug
log_msg "=== STARTING ZENODO DEPOSITION CREATION ==="
log_msg "Token provided: ${ZENODO_TOKEN:0:10}..." 
log_msg "About to call Zenodo API..."

# Create Zenodo deposition first
log_msg "Creating empty Zenodo deposition first..."

# First, create an empty deposition using curl directly
log_msg "Calling Zenodo API..."

# Debug: use separate calls to get cleaner output
CURL_OUTPUT=$(mktemp)
HTTP_CODE=$(curl -s -w "%{http_code}" \
    -H "Content-Type: application/json" \
    -X POST \
    "https://zenodo.org/api/deposit/depositions?access_token=${ZENODO_TOKEN}" \
    -d '{}' \
    -o "$CURL_OUTPUT")

RESPONSE_BODY=$(cat "$CURL_OUTPUT")
rm -f "$CURL_OUTPUT"

log_msg "Deposition creation HTTP code: $HTTP_CODE"
log_msg "Response body length: ${#RESPONSE_BODY} characters"
log_msg "Response body: $RESPONSE_BODY"

if [ "$HTTP_CODE" != "201" ]; then
    log_msg "Error: Failed to create Zenodo deposition (HTTP $HTTP_CODE)"
    
    # Try to parse the error message from JSON
    if command -v python >/dev/null 2>&1; then
        ERROR_MSG=$(echo "$RESPONSE_BODY" | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('message', 'Unknown error'))" 2>/dev/null)
        if [ -n "$ERROR_MSG" ]; then
            log_msg "Error message: $ERROR_MSG"
        fi
    fi
    
    log_msg "Full response: $RESPONSE_BODY"
    log_msg ""
    log_msg "Common fixes for 403 Permission denied:"
    log_msg "1. Check that your token has 'deposit:write' and 'deposit:actions' scopes"
    log_msg "2. Make sure you're using the sandbox token for sandbox.zenodo.org"
    log_msg "3. Try creating the token again at: https://zenodo.org/account/settings/applications/tokens/new/"
    exit 1
fi

log_msg "Parsing JSON response..."

# Parse the JSON response to extract deposition ID and bucket URL
DEPOSITION_ID=$(echo "$RESPONSE_BODY" | grep -o '"id": *[0-9]*' | grep -o '[0-9]*')
BUCKET_URL=$(echo "$RESPONSE_BODY" | grep -o '"bucket": *"[^"]*"' | cut -d'"' -f4)

log_msg "Raw deposition ID match: $(echo "$RESPONSE_BODY" | grep -o '"id": *[0-9]*')"
log_msg "Raw bucket URL match: $(echo "$RESPONSE_BODY" | grep -o '"bucket": *"[^"]*"')"

if [ -z "$DEPOSITION_ID" ] || [ -z "$BUCKET_URL" ]; then
    log_msg "Error: Could not extract deposition ID or bucket URL from JSON"
    log_msg "Deposition ID: '$DEPOSITION_ID'"
    log_msg "Bucket URL: '$BUCKET_URL'"
    log_msg "Full response body:"
    log_msg "$RESPONSE_BODY"
    exit 1
fi

log_msg "Successfully created deposition ID: $DEPOSITION_ID"
log_msg "Bucket URL: $BUCKET_URL"

# Test the bucket URL format
if [[ "$BUCKET_URL" =~ ^https://.*zenodo.*api/files/.* ]]; then
    log_msg "Bucket URL format looks correct"
else
    log_msg "Warning: Bucket URL format may be incorrect: $BUCKET_URL"
fi

# Now update the metadata using curl
log_msg "Updating deposition metadata..."
METADATA_JSON='{
    "metadata": {
        "title": "'"$ZENODO_TITLE"'",
        "upload_type": "dataset",
        "description": "'"$ZENODO_DESCRIPTION"'",
        "creators": [{"name": "CASES Team", "affiliation": "Research Institution"}],
        "keywords": ["CASES", "scintillation", "GPS", "ionosphere"]
    }
}'

METADATA_RESPONSE=$(curl -s -w "%{http_code}\n" \
    -H "Content-Type: application/json" \
    -X PUT \
    "https://zenodo.org/api/deposit/depositions/${DEPOSITION_ID}?access_token=${ZENODO_TOKEN}" \
    -d "$METADATA_JSON")

METADATA_HTTP_CODE=$(echo "$METADATA_RESPONSE" | tail -n 1)

if [ "$METADATA_HTTP_CODE" != "200" ]; then
    log_msg "Warning: Failed to update metadata (HTTP $METADATA_HTTP_CODE), but continuing with uploads"
    log_msg "Metadata response: $(echo "$METADATA_RESPONSE" | head -n -1)"
else
    log_msg "Successfully updated metadata"
fi

# Track the first deposition ID
ALL_DEPOSITION_IDS+=("$DEPOSITION_ID")
log_msg "Tracking deposition Part 1: $DEPOSITION_ID"

# Copy binflate executable to working directory
BINFLATE_PATH="/data1/public/Data/cases/$FIRMWARE_VERSION/binflate"
if [ -f "$BINFLATE_PATH" ]; then
    cp "$BINFLATE_PATH" ./binflate
    chmod +x binflate
    log_msg "Copied binflate to working directory"
else
    log_msg "Error: binflate not found at $BINFLATE_PATH"
    exit 1
fi

# Function to create and upload archive from staging directory
create_and_upload_archive() {
    local archive_name="$1"
    local archive_num="$2"

    if [ ! -d "$STAGING_DIR" ] || [ $(count_files "$STAGING_DIR") -eq 0 ]; then
        log_msg "No files in staging directory to archive"
        return 0
    fi

    local staging_size=$(get_size_gb "$STAGING_DIR")
    local file_count=$(count_files "$STAGING_DIR")

    log_msg "Creating archive $archive_num: $archive_name"
    log_msg "  Files to archive: $file_count"
    log_msg "  Total size: ${staging_size}GB"

    # Create the archive in the script start directory
    cd "$SCRIPT_START_DIR"
    tar -czf "$archive_name" -C "$STAGING_DIR" .

    if [ $? -eq 0 ]; then
        local archive_size=$(ls -lh "$archive_name" | awk '{print $5}')
        log_msg "  Archive created successfully: $archive_size"

        # Upload to Zenodo
        if upload_to_zenodo "$archive_name"; then
            log_msg "  Successfully uploaded: $archive_name"
            log_msg "  Local archive preserved: $archive_name"

            # Clear staging directory for next batch (files are now in the archive)
            rm -rf "$STAGING_DIR"/*
            log_msg "  Cleared staging directory"
            return 0
        else
            log_msg "  Error: Failed to upload $archive_name"
            log_msg "  Local archive preserved for retry: $archive_name"
            return 1
        fi
    else
        log_msg "  Error: Failed to create archive $archive_name"
        return 1
    fi
}

# Helper function to check if a year is a leap year
is_leap_year() {
    local year=$1
    if (( (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) )); then
        return 0  # true
    else
        return 1  # false
    fi
}

# Helper function to get days in a year
days_in_year() {
    local year=$1
    if is_leap_year "$year"; then
        echo 366
    else
        echo 365
    fi
}

# Helper function to get previous day (returns "year doy")
get_prev_day() {
    local year=$1
    local doy=$2

    if [ $doy -eq 1 ]; then
        # Previous year, last day
        local prev_year=$((year - 1))
        local prev_doy=$(days_in_year $prev_year)
        echo "$prev_year $prev_doy"
    else
        echo "$year $((doy - 1))"
    fi
}

# Helper function to get next day (returns "year doy")
get_next_day() {
    local year=$1
    local doy=$2
    local max_doy=$(days_in_year $year)

    if [ $doy -eq $max_doy ]; then
        # Next year, first day
        echo "$((year + 1)) 1"
    else
        echo "$year $((doy + 1))"
    fi
}

# Function to find all bin files for a receiver/year/doy across all base paths
# Returns a list of unique bin files (by name), choosing the largest when duplicates exist
find_bin_files_all_locations() {
    local year=$1
    local doy=$2
    local receiver=$3

    # Temporary file to track files: filename|size|full_path
    local temp_file=$(mktemp)

    # Determine which file patterns to search for based on BIN_TYPE
    local patterns=()
    case "$BIN_TYPE" in
        "dataout")
            patterns=("dataout_*.bin")
            ;;
        "dataoutiq")
            patterns=("dataoutiq_*.bin")
            ;;
        "both")
            patterns=("dataout_*.bin" "dataoutiq_*.bin")
            ;;
    esac

    # Search all base paths
    for base in "${BASE_PATHS[@]}"; do
        local bin_dir="${base}/${year}/${doy}/${receiver}/bin"

        # Skip if directory doesn't exist
        if [ ! -d "$bin_dir" ]; then
            continue
        fi

        # Check for bin files using the appropriate patterns
        local found_files=false
        for pattern in "${patterns[@]}"; do
            for binfile in "$bin_dir"/$pattern; do
                if [ -f "$binfile" ]; then
                    found_files=true
                    local filename=$(basename "$binfile")
                    local size=$(stat -c%s "$binfile" 2>/dev/null || stat -f%z "$binfile" 2>/dev/null || echo "0")
                    echo "${filename}|${size}|${binfile}" >> "$temp_file"
                fi
            done
        done

        if [ "$found_files" = true ]; then
            log_msg "  Found bin files in: $bin_dir"
        fi
    done

    # Process the temp file to find largest version of each unique filename
    # Sort by filename, then by size (descending), then take first of each filename
    if [ -s "$temp_file" ]; then
        sort -t'|' -k1,1 -k2,2rn "$temp_file" | \
        awk -F'|' '!seen[$1]++ {print $3 "|" $2}' | \
        while IFS='|' read -r filepath filesize; do
            log_msg "    Selected: $(basename "$filepath") (${filesize} bytes) from $(dirname "$filepath")"
            echo "$filepath"
        done
    fi

    rm -f "$temp_file"
}

# Function to upload file to Zenodo bucket
# Automatically creates new part deposition if size limit would be exceeded
upload_to_zenodo() {
    local file_path="$1"
    local filename=$(basename "$file_path")

    if [ ! -f "$file_path" ]; then
        log_msg "    Error: File not found: $file_path"
        return 1
    fi

    # Get size of file to upload (in bytes)
    local file_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null || echo "0")
    local file_size_human=$(ls -lh "$file_path" | awk '{print $5}')

    log_msg "  Preparing to upload $filename to Zenodo..."
    log_msg "    File size: $file_size_human ($file_size bytes)"

    # Check if this upload would exceed the deposition size limit
    local current_dep_size=$(get_zenodo_deposition_size "$DEPOSITION_ID")
    local projected_size=$((current_dep_size + file_size))

    log_msg "    Current deposition size: $((current_dep_size / 1024 / 1024)) MB"
    log_msg "    Projected size after upload: $((projected_size / 1024 / 1024)) MB"
    log_msg "    Max deposition size: $((MAX_ZENODO_DEPOSITION_BYTES / 1024 / 1024)) MB"

    # If we would exceed the limit, create a new part deposition
    if [ $projected_size -gt $MAX_ZENODO_DEPOSITION_BYTES ]; then
        log_msg "    WARNING: Upload would exceed ${MAX_ZENODO_DEPOSITION_GB}GB deposition limit!"
        log_msg "    Creating new deposition part..."

        DEPOSITION_PART=$((DEPOSITION_PART + 1))

        if ! create_new_part_deposition "$DEPOSITION_PART"; then
            log_msg "    ERROR: Failed to create new part deposition"
            return 1
        fi

        log_msg "    Now uploading to Part $DEPOSITION_PART (Deposition ID: $DEPOSITION_ID)"
    fi

    log_msg "    Target URL: ${BUCKET_URL}/${filename}"

    # Create temporary file for curl output
    CURL_OUTPUT=$(mktemp)
    CURL_HTTP_CODE=$(mktemp)

    # Use curl to upload directly to bucket with more detailed error reporting
    curl -s -w "%{http_code}" \
        -X PUT \
        -T "$file_path" \
        -H "Content-Type: application/octet-stream" \
        "${BUCKET_URL}/${filename}?access_token=${ZENODO_TOKEN}" \
        -o "$CURL_OUTPUT" \
        2>"$CURL_HTTP_CODE.err" >"$CURL_HTTP_CODE"

    local curl_exit_code=$?
    local http_code=$(cat "$CURL_HTTP_CODE" 2>/dev/null)
    local curl_stderr=$(cat "$CURL_HTTP_CODE.err" 2>/dev/null)
    local response_body=$(cat "$CURL_OUTPUT" 2>/dev/null)

    # Debug output
    log_msg "    Curl exit code: $curl_exit_code"
    log_msg "    HTTP code: '$http_code'"

    if [ $curl_exit_code -ne 0 ]; then
        log_msg "    Curl error: $curl_stderr"
        rm -f "$CURL_OUTPUT" "$CURL_HTTP_CODE" "$CURL_HTTP_CODE.err"
        return 1
    fi

    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
        log_msg "    Successfully uploaded: $filename"
        log_msg "    Response: $response_body"
        rm -f "$CURL_OUTPUT" "$CURL_HTTP_CODE" "$CURL_HTTP_CODE.err"
        return 0
    else
        log_msg "    Error uploading $filename: HTTP $http_code"
        log_msg "    Response: $response_body"
        log_msg "    Stderr: $curl_stderr"
        rm -f "$CURL_OUTPUT" "$CURL_HTTP_CODE" "$CURL_HTTP_CODE.err"
        return 1
    fi
}

# Initialize counters
event_num=0
total_files_processed=0
total_archives_uploaded=0
archive_counter=1

# Convert GB limit to bytes for comparison
MAX_ARCHIVE_SIZE_BYTES=$((MAX_ARCHIVE_SIZE_GB * 1024 * 1024 * 1024))

# Process each event
while read -r year doy signal prn start_hour start_min end_hour end_min; do
    event_num=$((event_num + 1))

    # Skip empty lines
    [ -z "$year" ] && continue

    # Format DOY with leading zeros
    doy_formatted=$(printf "%03d" $doy)

    # Add 1-hour buffer before and after event (with cross-day handling)
    need_prev_day=false
    need_next_day=false
    prev_day_year=""
    prev_day_doy=""
    next_day_year=""
    next_day_doy=""

    # Calculate buffered hours for the main day
    buffered_start_hour=$((start_hour - 1))
    buffered_end_hour=$((end_hour + 1))

    # Check if we need previous day data (buffer extends before midnight)
    if [ $buffered_start_hour -lt 0 ]; then
        need_prev_day=true
        read prev_day_year prev_day_doy <<< $(get_prev_day $year $doy)
        prev_day_doy_formatted=$(printf "%03d" $prev_day_doy)
        buffered_start_hour=0  # Cap at 0 for main day
    fi

    # Check if we need next day data (buffer extends past midnight)
    if [ $buffered_end_hour -gt 23 ]; then
        need_next_day=true
        read next_day_year next_day_doy <<< $(get_next_day $year $doy)
        next_day_doy_formatted=$(printf "%03d" $next_day_doy)
        buffered_end_hour=23  # Cap at 23 for main day
    fi

    # Create event folder name
    event_folder="Event_$(printf "%03d" $event_num)_${year}_${doy_formatted}_L${signal}_PRN${prn}_$(printf "%02d%02d" $start_hour $start_min)-$(printf "%02d%02d" $end_hour $end_min)"
    event_staging_dir="$STAGING_DIR/$event_folder"

    log_msg ""
    log_msg "=================================================="
    log_msg "Event $event_num/$TOTAL_EVENTS: Year $year, DOY $doy_formatted"
    log_msg "Time window: ${start_hour}:${start_min} - ${end_hour}:${end_min}"
    log_msg "Buffered window: ${buffered_start_hour}:00 - ${buffered_end_hour}:59 (+/- 1 hour)"
    if [ "$need_prev_day" = true ]; then
        log_msg "  + Previous day: ${prev_day_year}/${prev_day_doy_formatted} hour 23"
    fi
    if [ "$need_next_day" = true ]; then
        log_msg "  + Next day: ${next_day_year}/${next_day_doy_formatted} hour 00"
    fi
    log_msg "Signal: L$signal, PRN: $prn"
    log_msg "Event folder: $event_folder"
    log_msg "=================================================="

    # Create event-specific directory
    mkdir -p "$event_staging_dir"
    
    # Process each receiver
    for receiver in "${RECEIVERS[@]}"; do
        log_msg ""
        log_msg "Processing receiver: $receiver"

        # Find all bin files for this receiver/year/doy across all locations
        log_msg "Searching for bin files across all data locations..."
        bin_files_found=$(find_bin_files_all_locations "$year" "$doy_formatted" "$receiver")

        # Also search previous day if buffer extends before midnight
        prev_day_bin_files=""
        if [ "$need_prev_day" = true ]; then
            log_msg "Searching previous day ${prev_day_year}/${prev_day_doy_formatted} for hour 23 data..."
            prev_day_bin_files=$(find_bin_files_all_locations "$prev_day_year" "$prev_day_doy_formatted" "$receiver")
        fi

        # Also search next day if buffer extends past midnight
        next_day_bin_files=""
        if [ "$need_next_day" = true ]; then
            log_msg "Searching next day ${next_day_year}/${next_day_doy_formatted} for hour 00 data..."
            next_day_bin_files=$(find_bin_files_all_locations "$next_day_year" "$next_day_doy_formatted" "$receiver")
        fi

        # Combine all bin files
        all_bin_files="$bin_files_found"
        [ -n "$prev_day_bin_files" ] && all_bin_files="$all_bin_files"$'\n'"$prev_day_bin_files"
        [ -n "$next_day_bin_files" ] && all_bin_files="$all_bin_files"$'\n'"$next_day_bin_files"

        if [ -z "$all_bin_files" ]; then
            log_msg "Warning: No bin files found for $receiver on $year/$doy_formatted (including adjacent days)"
            continue
        fi

        # Process bin files that match time window
        files_processed_this_receiver=0
        while IFS= read -r binfile; do
            if [ -f "$binfile" ]; then
                filename=$(basename "$binfile")

                # Extract time and type from filename
                if [[ $filename =~ dataout(iq)?_([0-9]{4})_([0-9]{3})_([0-9]{4})\.bin ]]; then
                    # BASH_REMATCH[1] = "iq" or empty, [2] = year, [3] = doy, [4] = time
                    file_type="${BASH_REMATCH[1]}"
                    bin_year="${BASH_REMATCH[2]}"
                    bin_doy="${BASH_REMATCH[3]}"
                    bin_time="${BASH_REMATCH[4]}"
                    time_str="$bin_time"
                    file_hour=$((10#${time_str:0:2}))

                    # Check if file hour overlaps with buffered time window (with cross-day handling)
                    in_window=false

                    # Check if this is a file from the main event day
                    if [ "$bin_year" = "$year" ] && [ "$bin_doy" = "$doy_formatted" ]; then
                        # Main day: use buffered start/end hours
                        if [ $file_hour -ge $buffered_start_hour ] && [ $file_hour -le $buffered_end_hour ]; then
                            in_window=true
                        fi
                    # Check if this is a file from the previous day (only want hour 23)
                    elif [ "$need_prev_day" = true ] && [ "$bin_year" = "$prev_day_year" ] && [ "$bin_doy" = "$prev_day_doy_formatted" ]; then
                        if [ $file_hour -eq 23 ]; then
                            in_window=true
                        fi
                    # Check if this is a file from the next day (only want hour 00)
                    elif [ "$need_next_day" = true ] && [ "$bin_year" = "$next_day_year" ] && [ "$bin_doy" = "$next_day_doy_formatted" ]; then
                        if [ $file_hour -eq 0 ]; then
                            in_window=true
                        fi
                    fi

                    if [ "$in_window" = true ]; then
                        if [ "$file_type" = "iq" ]; then
                            log_msg "  Processing dataoutiq: $filename (hour $file_hour)"
                        else
                            log_msg "  Processing dataout: $filename (hour $file_hour)"
                        fi

                        # Copy bin file temporarily to working directory
                        temp_bin="temp_${filename}"
                        cp "$binfile" "$temp_bin"

                        # Run binflate
                        ./binflate -i "$temp_bin"

                        # Collect and copy log files to event staging
                        log_files_created=()
                        if [ "$file_type" = "iq" ]; then
                            # For dataoutiq files: process ONLY iq.log
                            if [ -f "iq.log" ]; then
                                new_name="${bin_year}_${bin_doy}_iq_${receiver}_${bin_time}.log"
                                cp "iq.log" "$event_staging_dir/$new_name"
                                log_files_created+=("$new_name")
                                rm -f "iq.log"
                            fi
                        else
                            # For dataout files: process all EXCEPT iq
                            for log_type in iono scint navsol channel txinfo; do
                                if [ -f "${log_type}.log" ]; then
                                    new_name="${bin_year}_${bin_doy}_${log_type}_${receiver}_${bin_time}.log"
                                    cp "${log_type}.log" "$event_staging_dir/$new_name"
                                    log_files_created+=("$new_name")
                                    rm -f "${log_type}.log"
                                fi
                            done
                        fi

                        if [ ${#log_files_created[@]} -gt 0 ]; then
                            log_msg "    Copied ${#log_files_created[@]} files to event folder: ${log_files_created[*]}"
                            total_files_processed=$((total_files_processed + ${#log_files_created[@]}))
                            files_processed_this_receiver=$((files_processed_this_receiver + ${#log_files_created[@]}))
                        fi

                        # Remove temporary bin file
                        rm -f "$temp_bin"
                    fi
                fi
            fi
        done <<< "$all_bin_files"

        if [ $files_processed_this_receiver -gt 0 ]; then
            log_msg "Success: Processed $files_processed_this_receiver files for $receiver"
        else
            log_msg "Warning: No files processed for $receiver"
        fi
    done
    
    log_msg "Completed event $event_num"

    # After completing each event, check if staging directory is getting too large
    staging_size_bytes=$(du -sb "$STAGING_DIR" 2>/dev/null | awk '{print $1}')
    staging_file_count=$(count_files "$STAGING_DIR")

    # Create archive if we're approaching the size limit
    if [ $staging_size_bytes -gt $MAX_ARCHIVE_SIZE_BYTES ]; then
        archive_name="cases_data_archive_${archive_counter}.tar.gz"
        log_msg ""
        log_msg "Staging directory size limit reached ($(get_size_gb "$STAGING_DIR")GB, $staging_file_count files)"

        if create_and_upload_archive "$archive_name" "$archive_counter"; then
            total_archives_uploaded=$((total_archives_uploaded + 1))
            archive_counter=$((archive_counter + 1))
        else
            log_msg "Error: Failed to create/upload archive $archive_counter"
        fi
    fi

done < "$EVENTS_FILE"

# Create final archive if there are remaining files in staging
staging_file_count=$(count_files "$STAGING_DIR")
if [ $staging_file_count -gt 0 ]; then
    log_msg ""
    log_msg "Creating final archive with remaining $staging_file_count files..."
    archive_name="cases_data_archive_${archive_counter}.tar.gz"

    if create_and_upload_archive "$archive_name" "$archive_counter"; then
        total_archives_uploaded=$((total_archives_uploaded + 1))
    else
        log_msg "Error: Failed to create/upload final archive"
    fi
fi

# Clean up
rm -f binflate
rm -rf "$STAGING_DIR"

log_msg ""
log_msg "=================================================="
log_msg "Processing completed!"
log_msg "Processed $event_num events"
log_msg "Total files processed: $total_files_processed"
log_msg "Total archives uploaded to Zenodo: $total_archives_uploaded"
log_msg "Total Zenodo depositions created: ${#ALL_DEPOSITION_IDS[@]}"
log_msg "Check log file: $LOG_FILE"
log_msg ""

if [ ${#ALL_DEPOSITION_IDS[@]} -eq 1 ]; then
    log_msg "Zenodo deposition ID: ${ALL_DEPOSITION_IDS[0]}"
    log_msg ""
    log_msg "To publish the deposition, visit:"
    log_msg "https://zenodo.org/deposit/${ALL_DEPOSITION_IDS[0]}"
else
    log_msg "Multi-part dataset created (${#ALL_DEPOSITION_IDS[@]} parts):"
    part_num=1
    for dep_id in "${ALL_DEPOSITION_IDS[@]}"; do
        log_msg "  Part $part_num: https://zenodo.org/deposit/$dep_id"
        part_num=$((part_num + 1))
    done
    log_msg ""
    log_msg "To publish all parts, visit each deposition link above"
    log_msg "Note: Remember to update metadata to cross-reference all parts before publishing"
fi
log_msg "=================================================="
