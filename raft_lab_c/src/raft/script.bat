@echo off
for /L %%i in (1,1,5) do (
    go test -race >> log.txt
)