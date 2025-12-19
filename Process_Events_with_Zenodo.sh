#!/bin/bash

# Process_Events_with_Zenodo.sh
# Main script to process CASES events and upload to Zenodo
# Usage: ./Process_Events_with_Zenodo.sh [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description] [bin_type] [staging_dir] [max_archive_size_gb] [grid_filter]

EVENTS_FILE=${1:-"List_of_events.txt"}
FIRMWARE_VERSION=${2:-"gss1400"}
ZENODO_TOKEN=${3}
ZENODO_TITLE=${4:-"CASES Scintillation Event Data"}
ZENODO_DESCRIPTION=${5:-"Processed CASES scintillation event data with IQ, ionospheric, scintillation, navigation, channel, and transmitter information."}
BIN_TYPE=${6:-"both"}  # Options: "dataout", "dataoutiq", "both"
STAGING_DIR=${7:-"./staging_$(date +%Y%m%d_%H%M%S)"}
MAX_ARCHIVE_SIZE_GB=${8:-45}  # Conservative limit under 50GB
GRID_FILTER=${9:-"all"}  # Grid filter: specific grid name or "all"
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
echo "Grid filter: $GRID_FILTER"
echo "Zenodo token: ${ZENODO_TOKEN:+[PROVIDED]}${ZENODO_TOKEN:-[NOT PROVIDED]}"
echo "Zenodo title: $ZENODO_TITLE"
echo ""

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
upload_to_zenodo() {
    local file_path="$1"
    local filename=$(basename "$file_path")
    
    if [ ! -f "$file_path" ]; then
        log_msg "    Error: File not found: $file_path"
        return 1
    fi
    
    log_msg "  Uploading $filename to Zenodo..."
    log_msg "    File size: $(ls -lh "$file_path" | awk '{print $5}')"
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

    # Create event folder name
    event_folder="Event_$(printf "%03d" $event_num)_${year}_${doy_formatted}_L${signal}_PRN${prn}_$(printf "%02d%02d" $start_hour $start_min)-$(printf "%02d%02d" $end_hour $end_min)"
    event_staging_dir="$STAGING_DIR/$event_folder"

    log_msg ""
    log_msg "=================================================="
    log_msg "Event $event_num/$TOTAL_EVENTS: Year $year, DOY $doy_formatted"
    log_msg "Time window: ${start_hour}:${start_min} - ${end_hour}:${end_min}"
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

        if [ -z "$bin_files_found" ]; then
            log_msg "Warning: No bin files found for $receiver on $year/$doy_formatted"
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

                    # Check if file hour overlaps with time window
                    in_window=false
                    if [ $file_hour -eq $start_hour ] || [ $file_hour -eq $end_hour ]; then
                        in_window=true
                    elif [ $file_hour -gt $start_hour ] && [ $file_hour -lt $end_hour ]; then
                        in_window=true
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
        done <<< "$bin_files_found"

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
log_msg "Zenodo deposition ID: $DEPOSITION_ID"
log_msg "Check log file: $LOG_FILE"
log_msg ""
log_msg "To publish the deposition, visit:"
log_msg "https://zenodo.org/deposit/$DEPOSITION_ID"
log_msg "Or use the publish API endpoint"
log_msg "=================================================="
