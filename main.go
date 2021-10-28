package main

import (
	"context"
	"github.com/jackc/pgconn"
	"log"
	"io"
	"errors"
	"fmt"
	"syscall"
)

func connect() *pgconn.PgConn {
	conn, err := pgconn.Connect(context.Background(), "dbname=test")
	if err != nil {
		log.Fatalf("copy error:%v", err)
	}
	return conn
}

type CopyBuffer struct {
	data []byte
	trigChan chan bool
	endChan chan error
}

func NewCopyBuffer() *CopyBuffer {
	buf := CopyBuffer{
		trigChan: make(chan bool),
		endChan: make(chan error),
	}
	return &buf
}

func (b *CopyBuffer) Write(p []byte) (n int, err error) {
	b.data = p
	b.trigChan <- true
	return len(p), nil
}

func (b *CopyBuffer) Read(p []byte) (n int, err error) {
	n = 0
	select {
	case <-b.trigChan:
		n = copy(p, b.data)
		return n, nil
	case err = <-b.endChan:
		return 0, err
	}
}

func (b *CopyBuffer) Done() {
	b.endChan <- io.EOF
}

func (b *CopyBuffer) Fail(s string) {
	b.endChan <- errors.New(s)
}

func userTime(t *syscall.Rusage) float64  {
    return float64(t.Utime.Sec) + 1e-6 * float64(t.Utime.Usec)
}

func systemTime(t *syscall.Rusage) float64 {
    return float64(t.Stime.Sec) + 1e-6*float64(t.Stime.Usec);
}

func main() {
	var err error

	src := connect()
	dest := connect()
	defer src.Close(context.Background())
	defer dest.Close(context.Background())
	buf := NewCopyBuffer()

	go func() {
		_, err = dest.CopyFrom(context.Background(), buf, "COPY pgbench_accounts_copy FROM stdin")
		if err != nil {
			log.Fatalf("copy from stdin failed:%v", err)
		}
	}()

	_, err = src.CopyTo(context.Background(), buf, "COPY ( select * from pgbench_accounts ) TO STDOUT")
	if err != nil {
		errText := fmt.Sprintf("copy to stdout failed: %v", err)
		buf.Fail(errText)
		log.Fatal(errText)
	}
	buf.Done()

	var usage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	log.Printf("  CPU time: %.06f sec user, %.06f sec system\n",
           userTime(&usage), systemTime(&usage));
	log.Printf("  Max resident memory size (kb): %d\n", usage.Maxrss);
}
