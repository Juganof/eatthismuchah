@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"

rem ---------- start-lokaal.bat: start de app lokaal ----------
rem Gebruik:  start-lokaal.bat           (alleen de app)
rem           start-lokaal.bat enrich    (eerst producten verrijken via curl, dan de app)

where npm >nul 2>nul
if errorlevel 1 (
    echo Node.js/npm niet gevonden. Installeer eerst Node.js van https://nodejs.org
    pause
    exit /b 1
)

if not exist node_modules (
    echo Eerste keer: dependencies installeren...
    call npm install --no-audit --no-fund
)

echo Lokale D1-database voorbereiden...
call npm run db:init:local

if /i "%~1"=="enrich" (
    if exist scripts\enrich-local.mjs (
        echo Producten verrijken via curl; dit kan een minuut duren...
        node scripts\enrich-local.mjs
    ) else (
        echo scripts\enrich-local.mjs niet gevonden; verrijking overgeslagen.
    )
)

echo Dev-server starten op http://127.0.0.1:8787 ...
start "AH Macro Planner (wrangler dev)" cmd /k "call ""node_modules\.bin\wrangler.cmd"" dev --port 8787"

echo Wachten tot de server klaar is...
set /a tries=0
:waitloop
set /a tries+=1
if %tries% gtr 40 (
    echo Server reageert niet na 80 seconden; kijk in het venster "AH Macro Planner (wrangler dev)".
    pause
    exit /b 1
)
timeout /t 2 /nobreak >nul 2>nul || ping -n 3 127.0.0.1 >nul
curl -s -o nul http://127.0.0.1:8787/ 2>nul
if errorlevel 1 goto waitloop

echo Server is klaar. Browser openen...
start "" http://127.0.0.1:8787/
echo.
echo De app draait. Sluit het venster "AH Macro Planner (wrangler dev)" om te stoppen.
endlocal
