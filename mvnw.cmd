@ECHO OFF
SETLOCAL EnableDelayedExpansion

set MAVEN_PROJECTBASEDIR=%~dp0
if "%MAVEN_PROJECTBASEDIR:~-1%"=="\" set MAVEN_PROJECTBASEDIR=%MAVEN_PROJECTBASEDIR:~0,-1%

set WRAPPER_DIR=%MAVEN_PROJECTBASEDIR%\.mvn\wrapper
set WRAPPER_JAR=%WRAPPER_DIR%\maven-wrapper.jar
set WRAPPER_PROPERTIES=%WRAPPER_DIR%\maven-wrapper.properties

if not exist "%WRAPPER_PROPERTIES%" (
  echo [ERROR] %WRAPPER_PROPERTIES% not found.
  exit /b 1
)

if not exist "%WRAPPER_DIR%" mkdir "%WRAPPER_DIR%"

if not exist "%WRAPPER_JAR%" (
  set WRAPPER_URL=
  for /f "tokens=1,* delims==" %%A in ('findstr /R "^wrapperUrl=" "%WRAPPER_PROPERTIES%"') do set WRAPPER_URL=%%B
  if "!WRAPPER_URL!"=="" (
    echo [ERROR] wrapperUrl is missing in %WRAPPER_PROPERTIES%.
    exit /b 1
  )

  echo Downloading Maven wrapper JAR from !WRAPPER_URL!
  powershell -NoProfile -ExecutionPolicy Bypass -Command "& { $ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '!WRAPPER_URL!' -OutFile '%WRAPPER_JAR%' }"
  if errorlevel 1 (
    echo [ERROR] Failed to download maven-wrapper.jar
    exit /b 1
  )
  if not exist "%WRAPPER_JAR%" (
    echo [ERROR] Failed to download maven-wrapper.jar
    exit /b 1
  )
)

if "%JAVA_HOME%"=="" (
  set JAVA_EXE=java
) else (
  set JAVA_EXE=%JAVA_HOME%\bin\java.exe
)

"%JAVA_EXE%" -cp "%WRAPPER_JAR%" "-Dmaven.multiModuleProjectDirectory=%MAVEN_PROJECTBASEDIR%" org.apache.maven.wrapper.MavenWrapperMain %*
exit /b %ERRORLEVEL%
