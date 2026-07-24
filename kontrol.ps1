<#
.SYNOPSIS
    mini-kafka kalite kapisi (PowerShell)
.DESCRIPTION
    Sirayla: gofmt -> go vet -> go test -race -> golangci-lint
    Hepsi gecerse 0, herhangi biri kalirsa 1 doner.
    Kurulu olmayan araclar atlanir, hata verilmez.
#>

$ErrorActionPreference = "Stop"
$HATA = 0
$ATLANAN = @()

function Yaz($renk, $metin) {
    switch ($renk) {
        "kirmizi" { Write-Host $metin -ForegroundColor Red }
        "yesil"   { Write-Host $metin -ForegroundColor Green }
        "gri"     { Write-Host $metin -ForegroundColor DarkGray }
        "baslik"  { Write-Host "`n== $metin ==" -ForegroundColor White }
        default   { Write-Host $metin }
    }
}

function Run-Step($etiket, $program, [string[]]$arguments) {
    Write-Host ("  {0,-28}" -f $etiket) -NoNewline

    # Capture both stdout and stderr; preserve $LASTEXITCODE
    # Use & with *>&1 to merge all streams, then convert to plain text
    $cikti = & $program $arguments 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $_.Exception.Message
        } else {
            "$_"
        }
    }
    $rc = $LASTEXITCODE
    $ciktiStr = $cikti | Out-String

    if ($rc -eq 0) {
        Yaz "yesil" "OK"
    } else {
        Yaz "kirmizi" "KALDI"
        $ciktiStr -split "`n" | Select-Object -Last 20 | ForEach-Object {
            if ($_) { Write-Host "    | $_" }
        }
        $script:HATA = 1
    }
}

function Run-Step-String($etiket, $komut) {
    Write-Host ("  {0,-28}" -f $etiket) -NoNewline

    # Use cmd /c to run the command string and preserve exit code
    $cikti = cmd /c $komut '2>&1'
    $rc = $LASTEXITCODE
    $ciktiStr = $cikti | Out-String

    if ($rc -eq 0) {
        Yaz "yesil" "OK"
    } else {
        Yaz "kirmizi" "KALDI"
        $ciktiStr -split "`n" | Select-Object -Last 20 | ForEach-Object {
            if ($_) { Write-Host "    | $_" }
        }
        $script:HATA = 1
    }
}

function Atla($etiket, $sebep) {
    Write-Host ("  {0,-28}" -f $etiket) -NoNewline
    Yaz "gri" "atlandi ($sebep kurulu degil)"
    $script:ATLANAN += $etiket
}

function Var($cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

# ============================================================================

Yaz "baslik" "1. Bicim kontrolu (gofmt)"

if (Var "gofmt") {
    Write-Host ("  {0,-28}" -f "gofmt -l") -NoNewline
    $FARK = & gofmt -l . 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $_.Exception.Message
        } else {
            "$_"
        }
    }
    $rc = $LASTEXITCODE
    $FARKStr = $FARK | Out-String

    if ($rc -ne 0) {
        Yaz "kirmizi" "KALDI (gofmt basarisiz)"
        $FARKStr -split "`n" | ForEach-Object {
            if ($_) { Write-Host "    | $_" }
        }
        $script:HATA = 1
    } elseif ([string]::IsNullOrWhiteSpace($FARKStr)) {
        Yaz "yesil" "OK"
    } else {
        Yaz "kirmizi" "KALDI"
        $FARKStr -split "`n" | ForEach-Object {
            if ($_) { Write-Host "    | $_" }
        }
        $script:HATA = 1
    }
} else {
    Atla "gofmt" "go"
}

# ============================================================================

Yaz "baslik" "2. Statik analiz (go vet)"

if (Var "go") {
    Run-Step "go vet ./..." "go" @("vet", "./...")
} else {
    Atla "go vet" "go"
}

# ============================================================================

Yaz "baslik" "3. Test"

if (Var "go") {
    # race detector cgo gerektirir; yoksa -race'siz calistir
    $cgo = [Environment]::GetEnvironmentVariable("CGO_ENABLED")
    if ($cgo -eq "1") {
        Run-Step "go test -race ./..." "go" @("test", "./...", "-race", "-count=1")
    } else {
        Run-Step "go test ./..." "go" @("test", "./...", "-count=1")
    }
} else {
    Atla "go test" "go"
}

# ============================================================================

Yaz "baslik" "4. Lint (golangci-lint)"

if (Var "golangci-lint") {
    Run-Step "golangci-lint run" "golangci-lint" @("run")
} else {
    Atla "golangci-lint" "golangci-lint"
}

# ============================================================================
Write-Host
if ($ATLANAN.Count -gt 0) {
    Yaz "gri" "Atlanan: $($ATLANAN -join ' ')"
}
if ($HATA -eq 0) {
    Yaz "yesil" "KAPI YESIL — butun adimlar gecti."
    exit 0
} else {
    Yaz "kirmizi" "KAPI KIRMIZI — yukaridaki adimlar duzeltilmeden gorev kapatilamaz."
    exit 1
}
