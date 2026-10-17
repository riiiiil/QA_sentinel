@echo off
call "%~dp0start.bat" --self-test
if errorlevel 1 exit /b 1
pause
