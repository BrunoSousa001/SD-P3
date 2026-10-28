param([string]$Prefixo = "rep04")
# Compila os quatro projetos (<prefixo>-produtor/-lider/-replica/-processador) para <projeto>\out
$ErrorActionPreference = "Stop"
foreach ($p in "produtor", "lider", "replica", "processador") {
    $dir = Join-Path $PSScriptRoot "$Prefixo-$p"
    $out = Join-Path $dir "out"
    if (Test-Path $out) { Remove-Item $out -Recurse -Force }
    New-Item -ItemType Directory -Path $out | Out-Null
    $fontes = Get-ChildItem (Join-Path $dir "src\$Prefixo") -Filter *.java | ForEach-Object { $_.FullName }
    & javac -encoding UTF-8 -d $out $fontes
    if ($LASTEXITCODE -ne 0) { throw "Falha a compilar $Prefixo-$p" }
    Write-Host "OK  $Prefixo-$p"
}
