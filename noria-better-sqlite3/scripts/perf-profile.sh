#!/bin/bash
#
# Easy perf profiling for Noria
#
# Usage:
#   ./scripts/perf-profile.sh node benchmark/perf-target.js
#   ./scripts/perf-profile.sh --flamegraph node benchmark/perf-target.js
#   ./scripts/perf-profile.sh --report-only    # analyze existing perf.data
#
# Requirements:
#   - Linux with perf installed
#   - sudo access (or perf_event_paranoid=0)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FLAMEGRAPH_DIR="/tmp/FlameGraph"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
GENERATE_FLAMEGRAPH=false
REPORT_ONLY=false
FREQUENCY=999

while [[ $# -gt 0 ]]; do
    case $1 in
        --flamegraph|-f)
            GENERATE_FLAMEGRAPH=true
            shift
            ;;
        --report-only|-r)
            REPORT_ONLY=true
            shift
            ;;
        --freq)
            FREQUENCY="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [options] <command>"
            echo ""
            echo "Options:"
            echo "  --flamegraph, -f    Generate flamegraph SVG"
            echo "  --report-only, -r   Analyze existing perf.data"
            echo "  --freq N            Sampling frequency (default: 999)"
            echo "  --help, -h          Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 node benchmark/perf-target.js"
            echo "  $0 --flamegraph node benchmark/lobsters.js"
            exit 0
            ;;
        *)
            break
            ;;
    esac
done

# Check for perf
if ! command -v perf &> /dev/null; then
    echo -e "${RED}Error: perf not found. Install with: sudo apt install linux-tools-generic${NC}"
    exit 1
fi

# Record if not report-only
if [ "$REPORT_ONLY" = false ]; then
    if [ $# -eq 0 ]; then
        echo -e "${RED}Error: No command specified${NC}"
        echo "Usage: $0 [options] <command>"
        exit 1
    fi

    echo -e "${GREEN}Recording profile...${NC}"
    echo -e "Command: $@"
    echo -e "Frequency: ${FREQUENCY} Hz"
    echo ""

    sudo perf record -g -F "$FREQUENCY" -- "$@"

    # Fix ownership
    sudo chown $(whoami):$(whoami) perf.data 2>/dev/null || true
fi

# Check perf.data exists
if [ ! -f "perf.data" ]; then
    echo -e "${RED}Error: perf.data not found${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}                    PERFORMANCE ANALYSIS                        ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Generate summary report
echo -e "${YELLOW}Top functions by self-time:${NC}"
echo ""
sudo perf report --stdio -g none --percent-limit 1 2>/dev/null | \
    awk '/^[[:space:]]+[0-9]/ && $2 > 0.5 {printf "  %6s  %-40s  %s\n", $2, $4, $5}' | \
    head -25

echo ""
echo -e "${YELLOW}Time by library:${NC}"
echo ""

# Aggregate by library
sudo perf report --stdio -g none 2>/dev/null | \
    awk '/^[[:space:]]+[0-9]/ && $2 > 0.1 {lib[$4]+=$2} END {for (l in lib) printf "  %6.1f%%  %s\n", lib[l], l}' | \
    sort -rn | head -10

# Hardware counters
echo ""
echo -e "${YELLOW}Hardware counters (re-running for stats):${NC}"
if [ "$REPORT_ONLY" = false ] && [ $# -gt 0 ]; then
    sudo perf stat -e cycles,instructions,cache-misses,branch-misses -- "$@" 2>&1 | \
        grep -E "(cycles|instructions|cache-misses|branch-misses|seconds)" | \
        sed 's/^/  /'
fi

# Generate flamegraph if requested
if [ "$GENERATE_FLAMEGRAPH" = true ]; then
    echo ""
    echo -e "${YELLOW}Generating flamegraph...${NC}"

    # Clone FlameGraph tools if needed
    if [ ! -d "$FLAMEGRAPH_DIR" ]; then
        git clone --depth 1 https://github.com/brendangregg/FlameGraph "$FLAMEGRAPH_DIR" 2>/dev/null
    fi

    OUTPUT="$PROJECT_DIR/benchmark/flamegraph-$(date +%Y%m%d-%H%M%S).svg"
    sudo perf script 2>/dev/null | \
        "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" 2>/dev/null | \
        "$FLAMEGRAPH_DIR/flamegraph.pl" --title "Noria Performance Profile" > "$OUTPUT"

    echo -e "  ${GREEN}Flamegraph saved to: $OUTPUT${NC}"
fi

echo ""
echo -e "${GREEN}Tips:${NC}"
echo "  - Use 'sudo perf report' for interactive exploration"
echo "  - Use '$0 --flamegraph <cmd>' for visual flamegraph"
echo "  - Self-time shows where CPU cycles are actually spent"
echo ""
