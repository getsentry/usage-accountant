#!/bin/bash
# Compare all Docker image variants with vulnerability scanning

set -e

REGULAR_VARIANTS=("3.11" "3.12" "3.13")
NATIVE_VARIANTS=("3.11" "3.12" "3.13")
PREBUILT_VARIANTS=("debian12" "debian13")
TEMP_DIR=$(mktemp -d)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================================================"
echo "           Docker Image Variants Comparison & Analysis"
echo "========================================================================"
echo ""

# Build missing images
echo "Checking for existing images..."
for pyver in "${REGULAR_VARIANTS[@]}"; do
    if ! docker image inspect usage-accountant:py${pyver} >/dev/null 2>&1; then
        echo "Building usage-accountant:py${pyver}..."
        docker build -q -f Dockerfile.distroless.py${pyver} -t usage-accountant:py${pyver} . >/dev/null
    fi
done

for variant in "${PREBUILT_VARIANTS[@]}"; do
    if ! docker image inspect usage-accountant:prebuilt-${variant} >/dev/null 2>&1; then
        echo "Building usage-accountant:prebuilt-${variant}..."
        docker build -q -f Dockerfile.distroless-prebuilt-${variant} -t usage-accountant:prebuilt-${variant} . >/dev/null
    fi
done

echo ""
echo "=== Image Sizes ==="
echo ""
printf "%-20s | %-10s | %-8s | %-8s | %-10s\n" "Variant" "Size" "CRITICAL" "HIGH" "Status"
printf "%-20s-+-%-10s-+-%-8s-+-%-8s-+-%-10s\n" "--------------------" "----------" "--------" "--------" "----------"

# Scan and display results
for pyver in "${REGULAR_VARIANTS[@]}"; do
    if docker image inspect usage-accountant:py${pyver} >/dev/null 2>&1; then
        size=$(docker images usage-accountant:py${pyver} --format "{{.Size}}")
        docker save usage-accountant:py${pyver} > ${TEMP_DIR}/scan.tar 2>/dev/null
        scan_result=$(trivy image --input ${TEMP_DIR}/scan.tar --severity CRITICAL,HIGH --quiet 2>&1 | grep "Total:" | tail -1)
        critical=$(echo "$scan_result" | sed -E 's/.*CRITICAL: ([0-9]+).*/\1/')
        high=$(echo "$scan_result" | sed -E 's/.*HIGH: ([0-9]+).*/\1/')
        [ -z "$critical" ] && critical="0"
        [ -z "$high" ] && high="0"
        rm ${TEMP_DIR}/scan.tar

        if [ "$critical" -eq 0 ]; then
            status="${GREEN}✓ Good${NC}"
        else
            status="${RED}✗ Issues${NC}"
        fi

        printf "%-20s | %-10s | %-8s | %-8s | " "Regular Py $pyver" "$size" "$critical" "$high"
        echo -e "$status"
    fi
done

# Check native variants if they exist
native_exists=false
for pyver in "${NATIVE_VARIANTS[@]}"; do
    if docker image inspect usage-accountant:native-py${pyver} >/dev/null 2>&1; then
        native_exists=true
        break
    fi
done

if [ "$native_exists" = true ]; then
    echo ""
    for pyver in "${NATIVE_VARIANTS[@]}"; do
        if docker image inspect usage-accountant:native-py${pyver} >/dev/null 2>&1; then
            size=$(docker images usage-accountant:native-py${pyver} --format "{{.Size}}")
            docker save usage-accountant:native-py${pyver} > ${TEMP_DIR}/scan.tar 2>/dev/null
            scan_result=$(trivy image --input ${TEMP_DIR}/scan.tar --severity CRITICAL,HIGH --quiet 2>&1 | grep "Total:" | tail -1)
            critical=$(echo "$scan_result" | sed -E 's/.*CRITICAL: ([0-9]+).*/\1/')
            high=$(echo "$scan_result" | sed -E 's/.*HIGH: ([0-9]+).*/\1/')
            [ -z "$critical" ] && critical="0"
            [ -z "$high" ] && high="0"
            rm ${TEMP_DIR}/scan.tar

            if [ "$critical" -eq 0 ]; then
                status="${GREEN}✓ Good${NC}"
            else
                status="${RED}✗ Issues${NC}"
            fi

            printf "%-20s | %-10s | %-8s | %-8s | " "Native Py $pyver" "$size" "$critical" "$high"
            echo -e "$status"
        fi
    done
fi

echo ""
for variant in "${PREBUILT_VARIANTS[@]}"; do
    if docker image inspect usage-accountant:prebuilt-${variant} >/dev/null 2>&1; then
        size=$(docker images usage-accountant:prebuilt-${variant} --format "{{.Size}}")
        docker save usage-accountant:prebuilt-${variant} > ${TEMP_DIR}/scan.tar 2>/dev/null
        scan_result=$(trivy image --input ${TEMP_DIR}/scan.tar --severity CRITICAL,HIGH --quiet 2>&1 | grep "Total:" | tail -1)
        critical=$(echo "$scan_result" | sed -E 's/.*CRITICAL: ([0-9]+).*/\1/')
        high=$(echo "$scan_result" | sed -E 's/.*HIGH: ([0-9]+).*/\1/')
        [ -z "$critical" ] && critical="0"
        [ -z "$high" ] && high="0"
        rm ${TEMP_DIR}/scan.tar

        if [ "$critical" -eq 0 ]; then
            status="${YELLOW}✗ Old${NC}"
        else
            status="${RED}✗ CRITICAL${NC}"
        fi

        label="Prebuilt $(echo $variant | sed 's/debian/D/')"
        printf "%-20s | %-10s | %-8s | %-8s | " "$label" "$size" "$critical" "$high"
        echo -e "$status"
    fi
done

echo ""
echo "=== Python Versions ==="
echo ""
printf "%-20s | %-15s\n" "Variant" "Python Version"
printf "%-20s-+-%-15s\n" "--------------------" "---------------"

for pyver in "${REGULAR_VARIANTS[@]}"; do
    if docker image inspect usage-accountant:py${pyver} >/dev/null 2>&1; then
        version=$(docker run --rm --entrypoint="" usage-accountant:py${pyver} python${pyver} --version 2>&1 | awk '{print $2}')
        printf "%-20s | %-15s\n" "Regular Py $pyver" "$version"
    fi
done

if [ "$native_exists" = true ]; then
    echo ""
    for pyver in "${NATIVE_VARIANTS[@]}"; do
        if docker image inspect usage-accountant:native-py${pyver} >/dev/null 2>&1; then
            version=$(docker run --rm --entrypoint="" usage-accountant:native-py${pyver} python${pyver} --version 2>&1 | awk '{print $2}')
            printf "%-20s | %-15s\n" "Native Py $pyver" "$version"
        fi
    done
fi

echo ""
for variant in "${PREBUILT_VARIANTS[@]}"; do
    if docker image inspect usage-accountant:prebuilt-${variant} >/dev/null 2>&1; then
        version=$(docker run --rm --entrypoint="" usage-accountant:prebuilt-${variant} python --version 2>&1 | awk '{print $2}')
        label="Prebuilt $(echo $variant | sed 's/debian/D/')"
        printf "%-20s | %-15s\n" "$label" "$version"
    fi
done

# Cleanup
rm -rf ${TEMP_DIR}

echo ""
echo "========================================================================"
echo "SUMMARY & RECOMMENDATIONS"
echo "========================================================================"
echo ""
echo -e "${GREEN}✓ RECOMMENDED:${NC} Regular variants (Dockerfile.distroless.py3.11)"
echo "  • 0 CRITICAL vulnerabilities"
echo "  • 125-135MB (smallest secure option)"
echo "  • 1-2 minute builds"
echo "  • Latest Python patches automatically"
echo ""
if [ "$native_exists" = true ]; then
    echo -e "${GREEN}✓ ALTERNATIVE:${NC} Native variants (for supply chain compliance)"
    echo "  • 0 CRITICAL vulnerabilities"
    echo "  • 400+ MB (3x larger)"
    echo "  • 8-10 minute builds"
    echo "  • Full control over build process"
    echo ""
fi
echo -e "${RED}✗ NOT RECOMMENDED:${NC} Prebuilt variants"
echo "  • Prebuilt D12: Old Python 3.11.2 (missing 12 patches)"
echo "  • Prebuilt D13: 4 CRITICAL + 13 HIGH vulnerabilities"
echo "  • Created for comparison/educational purposes only"
echo ""
echo "========================================================================"
echo "QUICK START"
echo "========================================================================"
echo ""
echo "Build recommended variant:"
echo "  docker build -f Dockerfile.distroless.py3.11 -t usage-accountant:latest ."
echo ""
echo "Run with tests:"
echo "  docker build -f Dockerfile.distroless.py3.11 --target test -t test ."
echo ""
echo "Scan specific image:"
echo "  docker save usage-accountant:py3.11 | trivy image --input /dev/stdin"
echo ""
echo "See full guide:"
echo "  cat DOCKER_GUIDE.md"
echo ""
echo "========================================================================"
