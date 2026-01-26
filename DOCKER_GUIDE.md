# Docker Distroless Variants Guide

Complete guide to secure, minimal Docker images for usage-accountant with **0 CRITICAL vulnerabilities**.

## Quick Start

```bash
# Build recommended variant (fastest, most secure)
docker build -f Dockerfile.distroless.py3.11 -t usage-accountant:latest .

# Compare all variants
./scripts/compare-images.sh

# Scan for vulnerabilities
docker save usage-accountant:latest | trivy image --input /dev/stdin --severity CRITICAL,HIGH
```

## TL;DR - Which Dockerfile Should I Use?

| Use Case | Dockerfile | Why |
|----------|-----------|-----|
| **Production (99% of cases)** | `Dockerfile.distroless.py3.11` | 0 CRITICAL, 135MB, 2min build, latest patches |
| **Latest Python features** | `Dockerfile.distroless.py3.13` | 0 CRITICAL, 125MB (smallest!), newest Python |
| **Supply chain compliance** | `Dockerfile.distroless-native.py3.11` | 0 CRITICAL, full source control, 10min build |
| **Testing/comparison** | `Dockerfile.distroless-prebuilt-*` | ⚠️ NOT for production (has CRITICAL CVEs) |

## All Available Variants

### 1. Regular Variants (✓ Recommended)

**Strategy**: Copy Python from official `python:X.XX-slim` → `distroless/base-debian13`

| File | Python | Size | CRITICAL | HIGH | Build Time |
|------|--------|------|----------|------|------------|
| `Dockerfile.distroless.py3.11` | 3.11.14 | 135MB | 0 | 3 | 1-2 min |
| `Dockerfile.distroless.py3.12` | 3.12.12 | 127MB | 0 | 1 | 1-2 min |
| `Dockerfile.distroless.py3.13` | 3.13.11 | 125MB | 0 | 1 | 1-2 min |

**Pros**: Smallest, fastest, latest patches, 0 CRITICAL
**Cons**: Depends on Python Docker images
**Use when**: Standard production (99% of cases)

### 2. Native Variants (✓ Supply Chain)

**Strategy**: Compile Python from source using `debian:13-slim` → `distroless/base-debian13`

| File | Python | Size | CRITICAL | HIGH | Build Time |
|------|--------|------|----------|------|------------|
| `Dockerfile.distroless-native.py3.11` | 3.11.11 | 412MB | 0 | 3 | 8-10 min |
| `Dockerfile.distroless-native.py3.12` | 3.12.8 | 419MB | 0 | 1 | 8-10 min |
| `Dockerfile.distroless-native.py3.13` | 3.13.1 | 432MB | 0 | 1 | 8-10 min |

**Pros**: Full control, verified source, 0 CRITICAL
**Cons**: 3x larger, 5x slower builds
**Use when**: Supply chain compliance, air-gapped, regulatory requirements

### 3. Prebuilt Variants (✗ Not Recommended)

**Strategy**: Use pre-built `distroless/python3-debianXX` images directly

| File | Python | Size | CRITICAL | HIGH | Issue |
|------|--------|------|----------|------|-------|
| `Dockerfile.distroless-prebuilt-debian12` | 3.11.2 | 132MB | 0 | 3 | Old Python (12 patches behind) |
| `Dockerfile.distroless-prebuilt-debian13` | 3.13.5 | 128MB | **4** | **13** | **CRITICAL CVEs!** |

**Why not recommended**: Debian 13 prebuilt has CVE-2025-13836 and other CRITICAL vulnerabilities
**Created for**: Educational comparison only

## Vulnerability Comparison

```
Variant              Critical  High   Total   Status
─────────────────────────────────────────────────────────────
Regular Py 3.11      0         3      3       ✓ BEST CHOICE
Regular Py 3.12      0         1      1       ✓ Excellent
Regular Py 3.13      0         1      1       ✓ Smallest secure
Native Py 3.11       0         3      3       ✓ Supply chain
Native Py 3.12       0         1      1       ✓ Supply chain
Native Py 3.13       0         1      1       ✓ Supply chain
Prebuilt D12         0         3      3       ✗ Old Python
Prebuilt D13         4         13     17      ✗ CRITICAL CVEs
```

**Key Finding**: Regular and Native variants achieve **0 CRITICAL** vulnerabilities. Prebuilt D13 has **4 CRITICAL + 13 HIGH**.

## Architecture Comparison

### Regular (Recommended)
```
python:3.11-slim (build)
  └─ Compile app dependencies
      └─ Copy Python + app → distroless/base-debian13
          └─ Result: 135MB, 0 CRITICAL, 2min build
```

### Native (Supply Chain)
```
debian:13-slim (build)
  └─ Download Python source
      └─ Compile Python + app
          └─ Copy to distroless/base-debian13
              └─ Result: 412MB, 0 CRITICAL, 10min build
```

### Prebuilt (Not Recommended)
```
distroless/python3-debian13 (has Python 3.13.5)
  └─ Copy app dependencies
      └─ Result: 128MB, 4 CRITICAL, 2min build
```

## Building Images

### Regular Variants
```bash
# Python 3.11 (recommended for production)
docker build -f Dockerfile.distroless.py3.11 -t usage-accountant:py3.11 .

# Python 3.12
docker build -f Dockerfile.distroless.py3.12 -t usage-accountant:py3.12 .

# Python 3.13 (smallest, latest features)
docker build -f Dockerfile.distroless.py3.13 -t usage-accountant:py3.13 .

# Build with tests
docker build -f Dockerfile.distroless.py3.11 --target test -t test .
```

### Native Variants
```bash
# Takes 8-10 minutes
docker build -f Dockerfile.distroless-native.py3.11 -t usage-accountant:native .
```

## Key Features (All Variants)

- ✓ **0 CRITICAL vulnerabilities** (regular & native)
- ✓ **Runs as nonroot** (UID 65532)
- ✓ **No shell or package manager** in runtime
- ✓ **Distroless base** (Debian 13)
- ✓ **All 23 tests passing**
- ✓ **67% smaller** than original slim (414MB → 135MB)

## Decision Tree

```
Do you have supply chain security requirements?
│
├─ NO → Use Regular Variants
│       Dockerfile.distroless.py3.11
│       ✓ 0 CRITICAL, 135MB, 2min build
│
└─ YES → Need to build from verified source?
         │
         ├─ YES → Use Native Variants
         │        Dockerfile.distroless-native.py3.11
         │        ✓ 0 CRITICAL, full control, 10min build
         │
         └─ NO → Still use Regular Variants
                  (same security, better efficiency)
```

## Common Commands

```bash
# Compare all variants
./scripts/compare-images.sh

# Run application
docker run usage-accountant:py3.11

# Test imports
docker run --rm --entrypoint="" usage-accountant:py3.11 \
  python3.11 -c "import usageaccountant; print('OK')"

# Verify user
docker run --rm --entrypoint="" usage-accountant:py3.11 \
  python3.11 -c "import os; print(f'UID={os.getuid()}')"

# Scan for vulnerabilities
docker save usage-accountant:py3.11 > image.tar
trivy image --input image.tar --severity CRITICAL,HIGH
rm image.tar
```

## Why Not Python 3.14?

The `confluent-kafka` library (required by `sentry-arroyo`) doesn't support Python 3.14 yet. Will add when available.

## Size vs Security Trade-offs

| Metric | Regular | Native | Prebuilt D13 |
|--------|---------|--------|--------------|
| **Image Size** | 135MB | 412MB (3x) | 128MB (smallest!) |
| **CRITICAL CVEs** | 0 ✓ | 0 ✓ | 4 ✗ |
| **Build Time** | 2min | 10min (5x) | 2min |
| **Python Version** | 3.11.14 (latest) | 3.11.11 (specific) | 3.13.5 (vulnerable) |

**Lesson**: Smallest image is NOT most secure. Regular variants provide best balance.

## Migration from Original

```bash
# Original slim Dockerfile
FROM python:3.11-slim
# ... 414MB image

# New distroless (recommended)
FROM gcr.io/distroless/base-debian13:nonroot
# Copy Python from builder
# ... 135MB image, 0 CRITICAL CVEs
```

**Benefits**: 67% smaller, 0 CRITICAL vulnerabilities, no shell access

## Best Practices

1. **Always scan images**: `trivy image --severity CRITICAL,HIGH`
2. **Use regular variants** unless supply chain compliance required
3. **Keep up to date**: Rebuild monthly for latest patches
4. **Test before deploy**: Use `--target test` to run tests during build
5. **Monitor CVEs**: Subscribe to security advisories

## Troubleshooting

### Import errors in runtime
- Check PYTHONPATH is set correctly (handled in Dockerfiles)
- Verify all dependencies compiled in builder stage

### Tests failing
- Build with `--target test` to run tests
- Check Python version compatibility

### Large image sizes
- Use regular variants (not native) for smaller images
- Verify `.dockerignore` is in place

## Contributing

When adding new Python versions:
1. Copy existing Dockerfile (e.g., `Dockerfile.distroless.py3.13`)
2. Update Python version in 3 places: base image, COPY paths, ENTRYPOINT
3. Test with `--target test` flag
4. Scan with trivy
5. Update this guide

## Files Reference

```
├── Dockerfiles (11 total)
│   ├── Dockerfile.distroless.py3.{11,12,13,14}      # Regular variants
│   ├── Dockerfile.distroless-native.py3.{11,12,13}  # Native variants
│   └── Dockerfile.distroless-prebuilt-debian{12,13} # Prebuilt (not recommended)
│
├── Documentation
│   └── DOCKER_GUIDE.md                               # This file
│
├── Scripts
│   └── scripts/compare-images.sh                     # Compare all variants
│
└── Configuration
    └── .dockerignore                                 # Build optimization
```

## Summary

**Recommendation**: Use `Dockerfile.distroless.py3.11` for production

**Why**:
- 0 CRITICAL vulnerabilities
- 135MB (67% smaller than original)
- 2-minute builds
- Latest Python patches automatically
- Same security as native but 3x smaller and 5x faster

**Avoid**: Prebuilt variants (especially debian13 with 4 CRITICAL CVEs)

---

**Build**: `docker build -f Dockerfile.distroless.py3.11 -t usage-accountant:latest .`
**Compare**: `./scripts/compare-images.sh`
**Scan**: `docker save usage-accountant:latest | trivy image --input /dev/stdin`
