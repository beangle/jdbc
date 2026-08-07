[CmdletBinding()]
param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$SqlArgs
)

$ErrorActionPreference = "Stop"

if (!$SqlArgs -or $SqlArgs.Count -eq 0) {
  Write-Host @"
Usage:
  sql.ps1 C:\path\to\workdir
"@
  exit 1
}

$remoteRepo = if ($env:M2_REMOTE_REPO) {
  $env:M2_REMOTE_REPO.TrimEnd("/")
} else {
  "https://maven.aliyun.com/repository/public"
}

$localRepo = if ($env:M2_REPO) {
  $env:M2_REPO
} else {
  Join-Path $HOME ".m2\repository"
}

$classpathEntries = [System.Collections.Generic.List[string]]::new()

function Add-MavenArtifact {
  param(
    [Parameter(Mandatory = $true)][string]$GroupId,
    [Parameter(Mandatory = $true)][string]$ArtifactId,
    [Parameter(Mandatory = $true)][string]$Version
  )

  $groupPath = $GroupId.Replace(".", "/")
  $relativePath = "$groupPath/$ArtifactId/$Version/$ArtifactId-$Version.jar"
  $localFile = Join-Path $localRepo ($relativePath.Replace("/", "\"))
  $classpathEntries.Add($localFile)

  if (!(Test-Path -LiteralPath $localFile -PathType Leaf)) {
    $targetDir = Split-Path -Parent $localFile
    New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

    $url = "$remoteRepo/$relativePath"
    $partialFile = "$localFile.part"
    Write-Host "fetching $url"
    try {
      Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $partialFile
      Move-Item -LiteralPath $partialFile -Destination $localFile -Force
    } catch {
      if (Test-Path -LiteralPath $partialFile) {
        Remove-Item -LiteralPath $partialFile -Force
      }
      throw "Cannot download $url`: $($_.Exception.Message)"
    }
  }
}

$scala3Ver = "3.3.8"
$beangleCommonsVer = "6.2.2"
$slf4jVer = "2.0.18"
$logbackVer = "1.6.1"
$bootVer = "0.1.28"
$beangleJdbcVer = if ($env:BEANGLE_JDBC_VER) { $env:BEANGLE_JDBC_VER } else { "1.1.11" }

Add-MavenArtifact "org.scala-lang" "scala3-library_3" $scala3Ver
Add-MavenArtifact "org.beangle.commons" "beangle-commons" $beangleCommonsVer
Add-MavenArtifact "org.beangle.boot" "beangle-boot" $bootVer
Add-MavenArtifact "org.slf4j" "slf4j-api" $slf4jVer
Add-MavenArtifact "ch.qos.logback" "logback-core" $logbackVer
Add-MavenArtifact "ch.qos.logback" "logback-classic" $logbackVer
Add-MavenArtifact "org.beangle.jdbc" "beangle-jdbc" $beangleJdbcVer

$sqlJar = Join-Path $localRepo "org\beangle\jdbc\beangle-jdbc\$beangleJdbcVer\beangle-jdbc-$beangleJdbcVer.jar"
$bootClasspath = $classpathEntries -join [IO.Path]::PathSeparator

& java -cp $bootClasspath org.beangle.boot.dependency.AppResolver $sqlJar "--remote=$remoteRepo" "--local=$localRepo" *> $null
if ($LASTEXITCODE -ne 0) {
  throw "Dependency resolution failed with exit code $LASTEXITCODE."
}

$launcherInfo = (& java -cp $bootClasspath org.beangle.boot.launcher.Classpath $sqlJar $localRepo | Out-String).Trim()
if ($LASTEXITCODE -ne 0) {
  throw "Classpath generation failed with exit code $LASTEXITCODE`: $launcherInfo"
}

$separator = $launcherInfo.IndexOf("@")
if ($separator -lt 1) {
  throw "Unexpected launcher output: $launcherInfo"
}

$mainClass = $launcherInfo.Substring(0, $separator)
$appClasspath = $launcherInfo.Substring($separator + 1)

& java -cp $appClasspath $mainClass @SqlArgs
exit $LASTEXITCODE
