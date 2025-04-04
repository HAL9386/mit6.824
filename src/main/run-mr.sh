#!/usr/bin/env bash

go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrcoordinator.go pg-*.txt

