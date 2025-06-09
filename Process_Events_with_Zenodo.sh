#!/bin/bash

# Process_Events_with_Zenodo.sh
# Main script to process CASES events and upload to Zenodo
# Usage: ./Process_Events_with_Zenodo.sh [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description]

EVENTS_FILE=${1:-"List_of_events.txt"}
FIRMWARE_VERSION=${2:-"gss1400"}
ZENODO_TOKEN=${3}
ZENODO_TITLE=${4:-"CASES Scintillation Event Data"}
ZENODO_DESCRIPTION=${5:-"Processed CASES scintillation event data with IQ, ionospheric, scintillation, navigation, channel, and transmitter information."}
BASE_PATH="/data1/public/Data/cases/pfrr"

# Store the original directory where script was run
SCRIPT_START_DIR="$(pwd)"

echo "CASES Data Processing and Zenodo Upload"
echo "======================================"
echo "Events file: $EVENTS_FILE"
echo "Firmware: $FIRMWARE_VERSION"
echo "Working directory: $SCRIPT_START_DIR"
echo "Zenodo token: ${ZENODO_TOKEN:+[PROVIDED]}${ZENODO_TOKEN:-[NOT PROVIDED]}"
echo "Zenodo title: $ZENODO_TITLE"
echo ""

# Check required parameters
if [ -z "$ZENODO_TOKEN" ]; then
    echo "Error: Zenodo token is required"
    echo "Usage: $0 [events_file] [firmware_version] [zenodo_token] [zenodo_title] [zenodo_description]"
    exit 1
fi

# Check if events file exists
if [ ! -f "$EVENTS_FILE" ]; then
    echo "Error: Events file not found: $EVENTS_FILE"
    exit 1
fi

# Count total events
TOTAL_EVENTS=$(wc -l < "$EVENTS_FILE")
echo "Total events to process: $TOTAL_EVENTS"
echo ""

# Default receivers
RECEIVERS=("grid108" "grid154" "grid160" "grid161" "grid162" "grid163")

# Create log file
LOG_FILE="processing_$(date +%Y%m%d_%H%M%S).log"
echo "Log file: $LOG_FILE"
echo ""

# Function to log messages
log_msg() {
    echo "$1" | tee -a "$LOG_FILE"
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

# Process each event
event_num=0
total_files_uploaded=0

while read -r year doy signal prn start_hour start_min end_hour end_min; do
    event_num=$((event_num + 1))
    
    # Skip empty lines
    [ -z "$year" ] && continue
    
    # Format DOY with leading zeros
    doy_formatted=$(printf "%03d" $doy)
    
    log_msg ""
    log_msg "=================================================="
    log_msg "Event $event_num/$TOTAL_EVENTS: Year $year, DOY $doy_formatted"
    log_msg "Time window: ${start_hour}:${start_min} - ${end_hour}:${end_min}"
    log_msg "Signal: L$signal, PRN: $prn"
    log_msg "=================================================="
    
    # Process each receiver
    for receiver in "${RECEIVERS[@]}"; do
        log_msg ""
        log_msg "Processing receiver: $receiver"
        
        # Build path to source data
        source_bin_dir="$BASE_PATH/$year/$doy_formatted/$receiver/bin"
        
        if [ ! -d "$source_bin_dir" ]; then
            log_msg "Warning: Directory not found: $source_bin_dir"
            continue
        fi
        
        # Check for bin files in source directory
        if ! ls "$source_bin_dir"/dataout_*.bin 1> /dev/null 2>&1; then
            log_msg "Warning: No bin files found in $source_bin_dir"
            continue
        fi
        
        # Process bin files that match time window
        archive_count=0
        for binfile in "$source_bin_dir"/dataout_*.bin; do
            if [ -f "$binfile" ]; then
                filename=$(basename "$binfile")
                
                # Extract time from filename
                if [[ $filename =~ dataout_[0-9]{4}_[0-9]{3}_([0-9]{4})\.bin ]]; then
                    time_str="${BASH_REMATCH[1]}"
                    file_hour=$((10#${time_str:0:2}))
                    
                    # Check if file hour overlaps with time window
                    in_window=false
                    if [ $file_hour -eq $start_hour ] || [ $file_hour -eq $end_hour ]; then
                        in_window=true
                    elif [ $file_hour -gt $start_hour ] && [ $file_hour -lt $end_hour ]; then
                        in_window=true
                    fi
                    
                    if [ "$in_window" = true ]; then
                        log_msg "  Processing: $filename (hour $file_hour contains window)"
                        
                        # Extract components from filename
                        if [[ $filename =~ dataout_([0-9]{4})_([0-9]{3})_([0-9]{4})\.bin ]]; then
                            bin_year="${BASH_REMATCH[1]}"
                            bin_doy="${BASH_REMATCH[2]}"
                            bin_time="${BASH_REMATCH[3]}"
                            
                            # Copy bin file temporarily to working directory
                            temp_bin="temp_${filename}"
                            cp "$binfile" "$temp_bin"
                            
                            # Run binflate
                            ./binflate -i "$temp_bin"
                            
                            # Collect log files
                            log_files_created=()
                            for log_type in iq iono scint navsol channel txinfo; do
                                if [ -f "${log_type}.log" ]; then
                                    new_name="${bin_year}_${bin_doy}_${log_type}_${receiver}_${bin_time}.log"
                                    mv "${log_type}.log" "$new_name"
                                    log_files_created+=("$new_name")
                                fi
                            done
                            
                            # Create combined archive
                            if [ ${#log_files_created[@]} -gt 0 ]; then
                                combined_name="${bin_year}_${bin_doy}_all_${receiver}_${bin_time}.tar.gz"
                                tar -czf "$combined_name" "${log_files_created[@]}"
                                log_msg "    Created: $combined_name"
                                
                                # Upload immediately to Zenodo
                                if upload_to_zenodo "$combined_name"; then
                                    total_files_uploaded=$((total_files_uploaded + 1))
                                    # Remove the local file after successful upload
                                    rm -f "$combined_name"
                                    log_msg "    Removed local file: $combined_name"
                                else
                                    log_msg "    Warning: Upload failed, keeping local file: $combined_name"
                                fi
                                
                                # Remove individual log files
                                rm -f "${log_files_created[@]}"
                                
                                archive_count=$((archive_count + 1))
                            fi
                            
                            # Remove temporary bin file
                            rm -f "$temp_bin"
                        fi
                    fi
                fi
            fi
        done
        
        if [ $archive_count -gt 0 ]; then
            log_msg "Success: Created and uploaded $archive_count files for $receiver"
        else
            log_msg "Warning: No files created for $receiver"
        fi
    done
    
    log_msg "Completed event $event_num"
    
done < "$EVENTS_FILE"

# Clean up
rm -f binflate

log_msg ""
log_msg "=================================================="
log_msg "Processing completed!"
log_msg "Processed $event_num events"
log_msg "Total files uploaded to Zenodo: $total_files_uploaded"
log_msg "Zenodo deposition ID: $DEPOSITION_ID"
log_msg "Check log file: $LOG_FILE"
log_msg ""
log_msg "To publish the deposition, visit:"
log_msg "https://zenodo.org/deposit/$DEPOSITION_ID"
log_msg "Or use the publish API endpoint"
log_msg "=================================================="
