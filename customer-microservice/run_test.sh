#!/bin/bash
#=========================================================================
# Customer Microservice JMeter Test Runner
#=========================================================================
# This script runs JMeter tests against the Customer API microservice
# with support for both local and distributed testing across VMs.
#=========================================================================

# Set strict error handling
set -e
trap 'echo "Error occurred at line $LINENO. Command: $BASH_COMMAND"' ERR

# Script constants
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JMETER_BIN="${JMETER_HOME:-/opt/apache-jmeter/bin}"
TEST_PLAN="${SCRIPT_DIR}/customer_storm_test.jmx"
PROPERTIES_FILE="${SCRIPT_DIR}/jmeter_test.properties"
RESULTS_DIR="${SCRIPT_DIR}/test-results"
LOG_DIR="${RESULTS_DIR}/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Default values
MODE="local"
SCENARIO="storm"
THREADS=10
DURATION=60
DEBUG=false
DISTRIBUTED=false
REMOTE_HOSTS=""
CLEAN=false
OPEN_RESULTS=false
VERBOSE=false
PAYLOAD_SIZE=512
STORM_OPS=20
STORM_CONCURRENCY=5

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print usage information
usage() {
    echo -e "${BLUE}Usage:${NC} $0 [options]"
    echo
    echo "Run JMeter tests against the Customer API microservice."
    echo
    echo -e "${YELLOW}Options:${NC}"
    echo "  -m, --mode MODE          Test mode: local or distributed (default: local)"
    echo "  -s, --scenario SCENARIO  Test scenario: storm, crud, payload, all (default: storm)"
    echo "  -t, --threads NUM        Number of threads to use (default: 10)"
    echo "  -d, --duration SEC       Test duration in seconds (default: 60)"
    echo "  -r, --remote HOSTS       Comma-separated list of remote JMeter servers (for distributed mode)"
    echo "  -p, --payload-size KB    Size of payload in KB for large payload tests (default: 512)"
    echo "  -o, --storm-ops NUM      Number of operations for connection storm (default: 20)"
    echo "  -c, --storm-conc NUM     Concurrency level for connection storm (default: 5)"
    echo "  -C, --clean              Clean results directory before running tests"
    echo "  -O, --open-results       Open results in browser after completion"
    echo "  -v, --verbose            Enable verbose output"
    echo "  -D, --debug              Enable debug mode"
    echo "  -h, --help               Display this help message"
    echo
    echo -e "${YELLOW}Examples:${NC}"
    echo "  $0 --mode local --scenario storm --threads 20 --duration 120"
    echo "  $0 --mode distributed --remote 192.168.1.101,192.168.1.102 --scenario all"
    echo "  $0 --scenario payload --payload-size 1024 --threads 5"
    echo
}

# Log messages with timestamp and color
log() {
    local level=$1
    local message=$2
    local color=$NC
    
    case $level in
        "INFO") color=$GREEN ;;
        "WARN") color=$YELLOW ;;
        "ERROR") color=$RED ;;
        "DEBUG") 
            color=$BLUE
            if [ "$DEBUG" != "true" ]; then
                return
            fi
            ;;
    esac
    
    echo -e "${color}[$(date +'%Y-%m-%d %H:%M:%S')] [$level] $message${NC}"
}

# Check if JMeter is installed and available
check_jmeter() {
    if [ ! -d "$JMETER_BIN" ]; then
        log "ERROR" "JMeter not found at $JMETER_BIN"
        log "ERROR" "Please install JMeter or set JMETER_HOME environment variable"
        exit 1
    fi
    
    if [ ! -f "${JMETER_BIN}/jmeter" ]; then
        log "ERROR" "JMeter executable not found at ${JMETER_BIN}/jmeter"
        exit 1
    }
    
    local jmeter_version=$(${JMETER_BIN}/jmeter --version 2>&1 | grep -oP "Version \K[0-9]+\.[0-9]+")
    log "INFO" "Found JMeter version $jmeter_version"
    
    # Check minimum version requirement
    if (( $(echo "$jmeter_version < 5.0" | bc -l) )); then
        log "WARN" "JMeter version $jmeter_version may be too old. Version 5.0+ recommended."
    fi
}

# Check if required files exist
check_files() {
    if [ ! -f "$TEST_PLAN" ]; then
        log "ERROR" "Test plan not found: $TEST_PLAN"
        exit 1
    fi
    
    if [ ! -f "$PROPERTIES_FILE" ]; then
        log "ERROR" "Properties file not found: $PROPERTIES_FILE"
        exit 1
    }
    
    log "INFO" "Test plan: $TEST_PLAN"
    log "INFO" "Properties file: $PROPERTIES_FILE"
}

# Check if the customer microservice is running
check_service() {
    local host=$(grep -oP "host=\K[^,\s]+" "$PROPERTIES_FILE" | head -1)
    local port=$(grep -oP "port=\K[0-9]+" "$PROPERTIES_FILE" | head -1)
    
    host=${host:-localhost}
    port=${port:-5000}
    
    log "INFO" "Checking if Customer API is running at $host:$port"
    
    # Try to connect to the service health endpoint
    if command -v curl &> /dev/null; then
        if curl -s "http://${host}:${port}/health" | grep -q "status"; then
            log "INFO" "Customer API is running"
            return 0
        fi
    elif command -v wget &> /dev/null; then
        if wget -q -O - "http://${host}:${port}/health" | grep -q "status"; then
            log "INFO" "Customer API is running"
            return 0
        fi
    else
        log "WARN" "Neither curl nor wget found, skipping service check"
        return 0
    fi
    
    log "ERROR" "Customer API is not running at $host:$port"
    log "ERROR" "Please start the service before running tests"
    exit 1
}

# Create necessary directories
setup_directories() {
    if [ "$CLEAN" = "true" ]; then
        log "INFO" "Cleaning results directory: $RESULTS_DIR"
        rm -rf "$RESULTS_DIR"
    fi
    
    mkdir -p "$RESULTS_DIR"
    mkdir -p "$LOG_DIR"
    
    # Create scenario-specific results directory
    local scenario_dir="${RESULTS_DIR}/${SCENARIO}_${TIMESTAMP}"
    mkdir -p "$scenario_dir"
    
    echo "$scenario_dir"
}

# Run JMeter in local mode
run_local_test() {
    local scenario_dir=$1
    local scenario_name=$2
    local additional_args=$3
    
    local results_file="${scenario_dir}/${scenario_name}_results.jtl"
    local log_file="${LOG_DIR}/${scenario_name}_${TIMESTAMP}.log"
    local report_dir="${scenario_dir}/report"
    
    mkdir -p "$report_dir"
    
    log "INFO" "Running $scenario_name test in local mode"
    log "INFO" "Results will be saved to: $results_file"
    
    local cmd="${JMETER_BIN}/jmeter -n -t \"$TEST_PLAN\" -p \"$PROPERTIES_FILE\" \
        -Jthreads=$THREADS -Jduration=$DURATION -Jresults_file=\"$results_file\" \
        -Jpayload_size=$PAYLOAD_SIZE -Jstorm_operations=$STORM_OPS -Jstorm_concurrency=$STORM_CONCURRENCY \
        $additional_args -l \"$results_file\" -j \"$log_file\" -e -o \"$report_dir\""
    
    log "DEBUG" "Executing command: $cmd"
    
    if [ "$VERBOSE" = "true" ]; then
        eval "$cmd"
    else
        eval "$cmd" > /dev/null
    fi
    
    if [ $? -eq 0 ]; then
        log "INFO" "Test completed successfully"
        log "INFO" "Results saved to: $results_file"
        log "INFO" "HTML report generated at: $report_dir"
        
        if [ "$OPEN_RESULTS" = "true" ]; then
            if command -v xdg-open &> /dev/null; then
                xdg-open "${report_dir}/index.html" &
            elif command -v open &> /dev/null; then
                open "${report_dir}/index.html" &
            else
                log "WARN" "Could not open results, no suitable command found"
            fi
        fi
    else
        log "ERROR" "Test failed, check logs for details: $log_file"
        return 1
    fi
}

# Run JMeter in distributed mode
run_distributed_test() {
    local scenario_dir=$1
    local scenario_name=$2
    local additional_args=$3
    
    local results_file="${scenario_dir}/${scenario_name}_results.jtl"
    local log_file="${LOG_DIR}/${scenario_name}_${TIMESTAMP}.log"
    local report_dir="${scenario_dir}/report"
    
    mkdir -p "$report_dir"
    
    log "INFO" "Running $scenario_name test in distributed mode"
    log "INFO" "Remote hosts: $REMOTE_HOSTS"
    log "INFO" "Results will be saved to: $results_file"
    
    local cmd="${JMETER_BIN}/jmeter -n -t \"$TEST_PLAN\" -p \"$PROPERTIES_FILE\" \
        -Jthreads=$THREADS -Jduration=$DURATION -Jresults_file=\"$results_file\" \
        -Jpayload_size=$PAYLOAD_SIZE -Jstorm_operations=$STORM_OPS -Jstorm_concurrency=$STORM_CONCURRENCY \
        -Jtest_mode=distributed -Jremote_hosts=$REMOTE_HOSTS \
        $additional_args -R $REMOTE_HOSTS -l \"$results_file\" -j \"$log_file\" -e -o \"$report_dir\""
    
    log "DEBUG" "Executing command: $cmd"
    
    if [ "$VERBOSE" = "true" ]; then
        eval "$cmd"
    else
        eval "$cmd" > /dev/null
    fi
    
    if [ $? -eq 0 ]; then
        log "INFO" "Distributed test completed successfully"
        log "INFO" "Results saved to: $results_file"
        log "INFO" "HTML report generated at: $report_dir"
        
        if [ "$OPEN_RESULTS" = "true" ]; then
            if command -v xdg-open &> /dev/null; then
                xdg-open "${report_dir}/index.html" &
            elif command -v open &> /dev/null; then
                open "${report_dir}/index.html" &
            else
                log "WARN" "Could not open results, no suitable command found"
            fi
        fi
    else
        log "ERROR" "Distributed test failed, check logs for details: $log_file"
        return 1
    fi
}

# Run connection storm test
run_storm_test() {
    local scenario_dir=$1
    local run_func=$2
    
    log "INFO" "Running connection storm test"
    log "INFO" "Operations: $STORM_OPS, Concurrency: $STORM_CONCURRENCY"
    
    # Disable all thread groups except Connection Storm Tests
    local additional_args="-JThreadGroup.0=false -JThreadGroup.1=false -JThreadGroup.2=false -JThreadGroup.3=true"
    
    $run_func "$scenario_dir" "connection_storm" "$additional_args"
}

# Run CRUD operations test
run_crud_test() {
    local scenario_dir=$1
    local run_func=$2
    
    log "INFO" "Running CRUD operations test"
    
    # Disable all thread groups except Customer CRUD Operations
    local additional_args="-JThreadGroup.0=false -JThreadGroup.1=true -JThreadGroup.2=false -JThreadGroup.3=false"
    
    $run_func "$scenario_dir" "crud_operations" "$additional_args"
}

# Run large payload test
run_payload_test() {
    local scenario_dir=$1
    local run_func=$2
    
    log "INFO" "Running large payload test with size: ${PAYLOAD_SIZE}KB"
    
    # Disable all thread groups except Large Payload Tests
    local additional_args="-JThreadGroup.0=false -JThreadGroup.1=false -JThreadGroup.2=true -JThreadGroup.3=false"
    
    $run_func "$scenario_dir" "large_payload" "$additional_args"
}

# Run all tests
run_all_tests() {
    local scenario_dir=$1
    local run_func=$2
    
    log "INFO" "Running all tests"
    
    # Run each test type
    run_storm_test "$scenario_dir" "$run_func"
    run_crud_test "$scenario_dir" "$run_func"
    run_payload_test "$scenario_dir" "$run_func"
    
    # Create a combined report
    log "INFO" "Generating combined report"
    local combined_dir="${scenario_dir}/combined_report"
    mkdir -p "$combined_dir"
    
    # Merge results files
    cat "${scenario_dir}"/*_results.jtl > "${scenario_dir}/combined_results.jtl"
    
    # Generate combined report
    ${JMETER_BIN}/jmeter -g "${scenario_dir}/combined_results.jtl" -o "$combined_dir" > /dev/null
    
    log "INFO" "Combined report generated at: $combined_dir"
    
    if [ "$OPEN_RESULTS" = "true" ]; then
        if command -v xdg-open &> /dev/null; then
            xdg-open "${combined_dir}/index.html" &
        elif command -v open &> /dev/null; then
            open "${combined_dir}/index.html" &
        fi
    fi
}

# Generate a summary report
generate_summary() {
    local scenario_dir=$1
    
    log "INFO" "Generating test summary"
    
    local summary_file="${scenario_dir}/summary.txt"
    
    {
        echo "========================================================"
        echo "  Customer Microservice JMeter Test Summary"
        echo "========================================================"
        echo "Date: $(date)"
        echo "Mode: $MODE"
        echo "Scenario: $SCENARIO"
        echo "Threads: $THREADS"
        echo "Duration: $DURATION seconds"
        echo "Payload Size: $PAYLOAD_SIZE KB"
        echo "Storm Operations: $STORM_OPS"
        echo "Storm Concurrency: $STORM_CONCURRENCY"
        if [ "$MODE" = "distributed" ]; then
            echo "Remote Hosts: $REMOTE_HOSTS"
        fi
        echo "========================================================"
        echo "Results Directory: $scenario_dir"
        echo "========================================================"
        
        # Add basic statistics if available
        if [ -f "${scenario_dir}/combined_results.jtl" ]; then
            echo "Statistics:"
            echo "  Total Requests: $(grep -c "<httpSample" "${scenario_dir}/combined_results.jtl")"
            echo "  Successful Requests: $(grep -c "s=\"true\"" "${scenario_dir}/combined_results.jtl")"
            echo "  Failed Requests: $(grep -c "s=\"false\"" "${scenario_dir}/combined_results.jtl")"
        elif [ -f "${scenario_dir}/${SCENARIO}_results.jtl" ]; then
            echo "Statistics:"
            echo "  Total Requests: $(grep -c "<httpSample" "${scenario_dir}/${SCENARIO}_results.jtl")"
            echo "  Successful Requests: $(grep -c "s=\"true\"" "${scenario_dir}/${SCENARIO}_results.jtl")"
            echo "  Failed Requests: $(grep -c "s=\"false\"" "${scenario_dir}/${SCENARIO}_results.jtl")"
        fi
        
        echo "========================================================"
    } > "$summary_file"
    
    log "INFO" "Summary saved to: $summary_file"
    cat "$summary_file"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m|--mode)
                MODE="$2"
                shift 2
                ;;
            -s|--scenario)
                SCENARIO="$2"
                shift 2
                ;;
            -t|--threads)
                THREADS="$2"
                shift 2
                ;;
            -d|--duration)
                DURATION="$2"
                shift 2
                ;;
            -r|--remote)
                REMOTE_HOSTS="$2"
                DISTRIBUTED=true
                MODE="distributed"
                shift 2
                ;;
            -p|--payload-size)
                PAYLOAD_SIZE="$2"
                shift 2
                ;;
            -o|--storm-ops)
                STORM_OPS="$2"
                shift 2
                ;;
            -c|--storm-conc)
                STORM_CONCURRENCY="$2"
                shift 2
                ;;
            -C|--clean)
                CLEAN=true
                shift
                ;;
            -O|--open-results)
                OPEN_RESULTS=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -D|--debug)
                DEBUG=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log "ERROR" "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Validate arguments
    if [[ ! "$MODE" =~ ^(local|distributed)$ ]]; then
        log "ERROR" "Invalid mode: $MODE. Must be 'local' or 'distributed'"
        exit 1
    fi
    
    if [[ ! "$SCENARIO" =~ ^(storm|crud|payload|all)$ ]]; then
        log "ERROR" "Invalid scenario: $SCENARIO. Must be 'storm', 'crud', 'payload', or 'all'"
        exit 1
    fi
    
    if [[ "$MODE" = "distributed" && -z "$REMOTE_HOSTS" ]]; then
        log "ERROR" "Distributed mode requires remote hosts (-r, --remote)"
        exit 1
    fi
    
    if ! [[ "$THREADS" =~ ^[0-9]+$ ]] || [ "$THREADS" -lt 1 ]; then
        log "ERROR" "Invalid thread count: $THREADS. Must be a positive integer"
        exit 1
    fi
    
    if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [ "$DURATION" -lt 1 ]; then
        log "ERROR" "Invalid duration: $DURATION. Must be a positive integer"
        exit 1
    fi
    
    if ! [[ "$PAYLOAD_SIZE" =~ ^[0-9]+$ ]] || [ "$PAYLOAD_SIZE" -lt 1 ]; then
        log "ERROR" "Invalid payload size: $PAYLOAD_SIZE. Must be a positive integer"
        exit 1
    fi
    
    if ! [[ "$STORM_OPS" =~ ^[0-9]+$ ]] || [ "$STORM_OPS" -lt 1 ]; then
        log "ERROR" "Invalid storm operations: $STORM_OPS. Must be a positive integer"
        exit 1
    fi
    
    if ! [[ "$STORM_CONCURRENCY" =~ ^[0-9]+$ ]] || [ "$STORM_CONCURRENCY" -lt 1 ]; then
        log "ERROR" "Invalid storm concurrency: $STORM_CONCURRENCY. Must be a positive integer"
        exit 1
    fi
}

# Main function
main() {
    log "INFO" "Starting Customer Microservice JMeter Test Runner"
    
    # Check JMeter installation
    check_jmeter
    
    # Check required files
    check_files
    
    # Check if the service is running
    check_service
    
    # Setup directories
    local scenario_dir=$(setup_directories)
    
    # Determine which run function to use based on mode
    local run_func="run_local_test"
    if [ "$MODE" = "distributed" ]; then
        run_func="run_distributed_test"
    fi
    
    # Run the appropriate test scenario
    case $SCENARIO in
        "storm")
            run_storm_test "$scenario_dir" "$run_func"
            ;;
        "crud")
            run_crud_test "$scenario_dir" "$run_func"
            ;;
        "payload")
            run_payload_test "$scenario_dir" "$run_func"
            ;;
        "all")
            run_all_tests "$scenario_dir" "$run_func"
            ;;
    esac
    
    # Generate summary
    generate_summary "$scenario_dir"
    
    log "INFO" "Test execution completed"
}

# Parse command line arguments
parse_args "$@"

# Run main function
main
