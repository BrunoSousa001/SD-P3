@REM Maven Wrapper CMD script
@echo off
setlocal
set "DIR=%~dp0"
if "%DIR%" == "" set "DIR=.\ "
set "WRAPPER_JAR=%DIR%.mvn\wrapper\maven-wrapper.jar"
if not exist "%WRAPPER_JAR%" (
    echo Error: Could not find %WRAPPER_JAR%
    exit /b 1
)
java -Dmaven.multiModuleProjectDirectory="%DIR%" -jar "%WRAPPER_JAR%" %*
