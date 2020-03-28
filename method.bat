setlocal

@REM modify if Anaconda is not installed to the user folder
set condapath=%HOMEDRIVE%%HOMEPATH%\Anaconda3\condabin\conda

@REM name of the Anaconda environment imported from cveureka.yml
set condaenv=cveureka

@REM config.ini should be in the project folder
set configpath=%cd%\config.ini

@REM change if the module script name is changed
set module=src.method

call %condapath% activate %condaenv%
python -m %module% %configpath%

PAUSE