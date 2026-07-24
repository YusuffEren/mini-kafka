#!/usr/bin/env bash
# ============================================================================
# kontrol.sh — mini-kafka kalite kapisi
#
# Sirayla: gofmt → go vet → go test -race → golangci-lint
# Hepsi gecerse 0, herhangi biri kalirsa 1 doner.
# Kurulu olmayan araclar atlanir, hata verilmez.
# ============================================================================
set -euo pipefail

HATA=0
ATLANAN=()

kirmizi() { printf '\033[31m%s\033[0m\n' "$*"; }
yesil()   { printf '\033[32m%s\033[0m\n' "$*"; }
gri()     { printf '\033[90m%s\033[0m\n' "$*"; }
baslik()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

var() { command -v "$1" >/dev/null 2>&1; }

# run_step <etiket> <komut...>
# Komutu calistirir, ciktisini yakalar. Basariliysa OK yazar.
# Basarisizsa KALDI yazar, ciktinin son 20 satirini gosterir, HATA=1 atar.
run_step() {
  local etiket="$1"; shift
  printf '  %-28s' "$etiket"

  # set +e ile gecici olarak -e'yi kapat: komut basarisiz olsa bile
  # ciktisini yakalayip raporlamak istiyoruz.
  set +e
  local cikti
  cikti="$("$@" 2>&1)"
  local rc=$?
  set -e

  if [ $rc -eq 0 ]; then
    yesil "OK"
  else
    kirmizi "KALDI"
    printf '%s\n' "$cikti" | tail -20 | sed 's/^/    | /'
    HATA=1
  fi
}

atla() {
  ATLANAN+=("$1")
  printf '  %-28s' "$1"; gri "atlandi ($2 kurulu degil)"
}

# ============================================================================
baslik "1. Bicim kontrolu (gofmt)"

if var gofmt; then
  printf '  %-28s' "gofmt -l"
  set +e
  local FARK
  FARK="$(gofmt -l . 2>&1)"
  local rc=$?
  set -e
  if [ $rc -ne 0 ]; then
    kirmizi "KALDI (gofmt basarisiz)"
    printf '%s\n' "$FARK" | sed 's/^/    | /'
    HATA=1
  elif [ -z "$FARK" ]; then
    yesil "OK"
  else
    kirmizi "KALDI"
    printf '%s\n' "$FARK" | sed 's/^/    | /'
    HATA=1
  fi
else
  atla "gofmt" "go"
fi

# ============================================================================
baslik "2. Statik analiz (go vet)"

if var go; then
  run_step "go vet ./..." go vet ./...
else
  atla "go vet" "go"
fi

# ============================================================================
baslik "3. Test (race detector ile)"

if var go; then
  run_step "go test -race ./..." go test ./... -race -count=1
else
  atla "go test" "go"
fi

# ============================================================================
baslik "4. Lint (golangci-lint)"

if var golangci-lint; then
  run_step "golangci-lint run" golangci-lint run
else
  atla "golangci-lint" "golangci-lint"
fi

# ============================================================================
echo
if [ ${#ATLANAN[@]} -gt 0 ]; then
  gri "Atlanan: ${ATLANAN[*]}"
fi
if [ "$HATA" -eq 0 ]; then
  yesil "KAPI YESIL — butun adimlar gecti."
  exit 0
else
  kirmizi "KAPI KIRMIZI — yukaridaki adimlar duzeltilmeden gorev kapatilamaz."
  exit 1
fi
