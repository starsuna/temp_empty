@echo off
setlocal
cd /d "%~dp0"

rem --- Pick a Python interpreter: bundled first, then a system install. ---
rem This makes the folder portable: move it anywhere, and as long as either
rem Apps\Python\python.exe travels with it OR Python is installed on the PC,
rem it runs.
set "PYEXE=%~dp0Apps\Python\python.exe"
if not exist "%PYEXE%" set "PYEXE=python"

echo ============================================
echo   HVAC Email Pipeline
echo ============================================
echo.
echo   1. Harvest HVAC companies from Google   (RESUMES where it left off)
echo   2. Crawl collected sites for emails      (RESUMES where it left off)
echo   3. Start over  (delete all collected data + progress, then exit)
echo.
set /p "CHOICE=Choose 1, 2, or 3: "

if "%CHOICE%"=="1" (
    "%PYEXE%" "%~dp0email_scraper.py"
    set "SCRAPER_EXIT=%ERRORLEVEL%"
    goto finish
)
if "%CHOICE%"=="2" (
    if not exist "%~dp0places.tsv" (
        echo.
        echo Missing places.tsv - run option 1 first to collect companies.
        set "SCRAPER_EXIT=2"
        goto finish
    )
    "%PYEXE%" "%~dp0website_crawler.py"
    set "SCRAPER_EXIT=%ERRORLEVEL%"
    goto finish
)
if "%CHOICE%"=="3" (
    call :del_file "places.tsv"
    call :del_file "emails.tsv"
    call :del_file "crawl_state.json"
    call :del_file "harvest_progress.json"
    echo.
    echo All collected data and progress cleared. Next run starts fresh.
    set "SCRAPER_EXIT=0"
    goto finish
)

echo Invalid choice.
set "SCRAPER_EXIT=2"
goto finish

:del_file
if exist "%~dp0%~1" del /q "%~dp0%~1"
exit /b 0

:finish
echo.
if "%SCRAPER_EXIT%"=="0" echo Done.
if "%SCRAPER_EXIT%"=="2" echo Stopped: API quota/key reached, or input missing. Re-run later to continue where it left off. See scraper.log.
if not "%SCRAPER_EXIT%"=="0" if not "%SCRAPER_EXIT%"=="2" echo Stopped early - see scraper.log for details.
echo.
pause
exit /b %SCRAPER_EXIT%
