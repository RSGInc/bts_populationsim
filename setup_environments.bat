
@echo on

mkdir sources
set WORKING_DIR=%~dp0
set ENV_YML=%WORKING_DIR%\bts_env.yml
set CONDA_DIR=C:\Users\%USERNAME%\AppData\Local\mambaforge\envs

if not exist %CONDA_DIR% (
	:: Alternatively, you can manually set a local mamba dir here
	set CONDA_DIR=C:\Users\nick.fournier\AppData\Local\mambaforge\envs
)

if not exist %CONDA_DIR% (
	echo Cannot find conda directory %CONDA_DIR%, edit this batch script!
	pause
	exit
)

call %CONDA_DIR%\Scripts\activate.bat %CONDA_DIR%

:: Install, stash source code in sources for debugging
if not exist src\populationsim\ (
	call git clone https://github.com/nick-fournier-rsg/populationsim.git ./src/populationsim
)

if not exist src\populationsim\ (
	call git clone https://github.com/ActivitySim/activitysim.git ./src/activitysim
)

if not exist %CONDA_DIR%\envs\bts_pop (
	call mamba env create -f %ENV_YML%
)
call mamba activate bts_pop

@REM call python -m pip install -e ./src/populationsim
@REM call python -m pip install -e --upgrade --force-reinstall ./src/activityim

pause
exit