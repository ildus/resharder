module github.com/ildus/resharder

go 1.17

replace github.com/jackc/pgconn => ../pgconn

require (
	github.com/jackc/pgconn v1.9.0
	github.com/jackc/pgproto3/v2 v2.1.1
)

require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/text v0.3.6 // indirect
)
