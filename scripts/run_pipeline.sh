#!/bin/bash
#
# Stock Picker Data Pipeline
# Runs all data fetching and analysis scripts in sequence.
# Exits immediately if any step fails.
#
# Usage:
#   cd scripts
#   ./run_pipeline.sh
#
# Or from project root:
#   ./scripts/run_pipeline.sh

set -e  # Exit immediately if any command fails
set -u  # Exit if undefined variable is used
set -o pipefail  # Fail if any command in a pipe fails

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and cd to it
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Stock Picker Data Pipeline${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Working directory: $(pwd)"
echo "Start time: $(date)"
echo ""

# Function to run a step and log it
run_step() {
    local step_num=$1
    local step_name=$2
    local script=$3
    
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}Step $step_num: $step_name${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "${YELLOW}Running: python $script${NC}"
    echo ""
    
    # Run the script and capture exit code
    if python "$script"; then
        echo -e "\n${GREEN}✓ Step $step_num completed successfully${NC}"
        return 0
    else
        local exit_code=$?
        echo -e "\n${RED}✗ Step $step_num FAILED with exit code: $exit_code${NC}"
        echo -e "${RED}Pipeline aborted.${NC}"
        exit $exit_code
    fi
}

# Execute pipeline steps
run_step 1 "Fetch Stock Prices (Split-Adjusted)" "fetch_stock_prices.py"
run_step 2 "Fetch Historical Earnings" "fetch_earnings.py"
run_step 3 "Fetch Upcoming Earnings" "fetch_upcoming_earnings.py"
run_step 4 "Fetch Income Statements" "fetch_income_statements.py"
run_step 5 "Run Minervini Analysis" "minervini.py"

# Success
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✓ Pipeline completed successfully!${NC}"
echo -e "${BLUE}========================================${NC}"
echo "End time: $(date)"
echo ""

exit 0
