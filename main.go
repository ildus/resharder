package main

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgio"
	"github.com/jackc/pgproto3/v2"
	"log"
	"io"
	"errors"
	"fmt"
	"syscall"
	"os"
	"bufio"
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

func copyUsingChannel() {
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

func ErrorResponseToPgError(msg *pgproto3.ErrorResponse) *pgconn.PgError {
	return &pgconn.PgError{
		Severity:         msg.Severity,
		Code:             string(msg.Code),
		Message:          string(msg.Message),
		Detail:           string(msg.Detail),
		Hint:             msg.Hint,
		Position:         msg.Position,
		InternalPosition: msg.InternalPosition,
		InternalQuery:    string(msg.InternalQuery),
		Where:            string(msg.Where),
		SchemaName:       string(msg.SchemaName),
		TableName:        string(msg.TableName),
		ColumnName:       string(msg.ColumnName),
		DataTypeName:     string(msg.DataTypeName),
		ConstraintName:   msg.ConstraintName,
		File:             string(msg.File),
		Line:             msg.Line,
		Routine:          string(msg.Routine),
	}
}

func manualCopy() {
	var query []byte
	var query2 []byte
	var err error

	src := connect()
	dest := connect()

	defer src.Close(context.Background())
	defer dest.Close(context.Background())

	resultReader := dest.Exec(context.Background(), "select pg_backend_pid()");
	results, err := resultReader.ReadAll()
	if err != nil {
		log.Fatalf("error:%v", err)
	}
	log.Printf("pid=%v, any key to continue..\n", string(results[0].Rows[0][0]))
	input := bufio.NewScanner(os.Stdin)
    input.Scan()
	log.Printf("ok")

	sql := "COPY ( select * from pgbench_accounts limit 3 ) TO STDOUT"
	query = (&pgproto3.Query{String: sql}).Encode(query)

	if err = src.SendBytes(context.Background(), query); err != nil {
		log.Fatalf("copy to stdout failed:%v", err)
	}

	sql2 := "COPY pgbench_accounts_copy FROM stdin"
	query2 = (&pgproto3.Query{String: sql2}).Encode(query2)

	if err = dest.SendBytes(context.Background(), query2); err != nil {
		log.Fatalf("copy from stdin failed:%v", err)
	}

	stopChan := make(chan bool)
	sigChan := make(chan bool)
	endChan := make(chan bool)

	go func() {
		var err error
		var msg pgproto3.BackendMessage

		first := true

loop:
		for {
			msg, err = dest.ReceiveMessage(context.Background())
			if err != nil {
				log.Fatalf("copy from stdin failed:%v", err)
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyInResponse:
				log.Println("dest: got copy in response");
			case *pgproto3.CopyOutResponse:
				log.Println("dest: got copy out response");
			case *pgproto3.ReadyForQuery:
				log.Println("dest: got ready for query");
				break loop
			case *pgproto3.ErrorResponse:
				pgErr := ErrorResponseToPgError(msg)
				log.Printf("dest: error response: %v\n", pgErr);
			default:
				log.Printf("dest msg: %+v\n", msg)
			}

			if (first) {
				stopChan <- true
				first = false
			}

			select {
			case <-sigChan:
				continue
			case <-endChan:
				break loop
			}
		}
	}()

	count := 0

	buf := make([]byte, 0, 65535)
	buf = append(buf, 'd')
	sp := len(buf)
	buf = buf[0 : 5]

loop:
	for {
		var msg pgproto3.BackendMessage
		msg, err = src.ReceiveMessage(context.Background())
		if err != nil {
			log.Fatalf("copy to stdout failed:%v", err)
		}

		if count == 0 {
			<-stopChan
		}
		count += 1

		switch msg := msg.(type) {
		case *pgproto3.CopyInResponse:
			log.Println("src: got copy in response");
		case *pgproto3.CopyOutResponse:
			log.Println("src: got copy out response");
		case *pgproto3.CopyDone:
			buf = buf[:0]
			log.Println("src: got copy done");
			copyDone := &pgproto3.CopyDone{}
			buf = copyDone.Encode(buf)
			log.Printf("%v", buf)

			if err = dest.SendBytes(context.Background(), buf); err != nil {
				log.Fatalf("sending to destination failed:%v", err)
			}
			sigChan <- true
		case *pgproto3.CopyData:
			log.Printf("src: got copy data, sending to dest: %s\n", string(msg.Data))

			buf = buf[0:5]
			pgio.SetInt32(buf[sp:], int32(len(msg.Data)+4))
			log.Printf("%v", buf)

			if err = dest.SendBytes(context.Background(), buf); err != nil {
				log.Fatalf("sending to destination failed:%v", err)
			}
			if err = dest.SendBytes(context.Background(), msg.Data); err != nil {
				log.Fatalf("sending to destination failed:%v", err)
			}
		case *pgproto3.ReadyForQuery:
			log.Println("src: got ready for query");
			break loop
		case *pgproto3.CommandComplete:
			log.Printf("src:command complete: %v\n", msg.CommandTag);
		case *pgproto3.ErrorResponse:
			pgErr := ErrorResponseToPgError(msg)
			log.Printf("src: error response: %v\n", pgErr);
		default:
			log.Printf("src msg: %+v\n", msg);
		}
	}

	log.Printf("any key to continue..\n")
	input = bufio.NewScanner(os.Stdin)
    input.Scan()
	endChan <- true
	log.Printf("ok")
}

func main() {
	manualCopy()
}
