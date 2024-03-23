#!/bin/bash
go install github.com/riverqueue/river/cmd/river@latest
river migrate-up --database-url "postgres://postgres:postgres@postgres/postgres"