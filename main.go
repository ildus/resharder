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
	"runtime/pprof"
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

func sendCopyFail(ctx context.Context, conn *pgconn.PgConn) error {
	buf := make([]byte, 0, 100)
	copyFail := &pgproto3.CopyFail{}
	buf = copyFail.Encode(buf)

	return conn.SendBytes(ctx, buf)
}

func sendCopyDone(ctx context.Context, conn *pgconn.PgConn) error {
	buf := make([]byte, 0, 100)
	copyDone := &pgproto3.CopyDone{}
	buf = copyDone.Encode(buf)

	return conn.SendBytes(ctx, buf)
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

	sql := "COPY ( select * from pgbench_accounts ) TO STDOUT"
	query = (&pgproto3.Query{String: sql}).Encode(query)

	if err = src.SendBytes(context.Background(), query); err != nil {
		log.Fatalf("copy to stdout failed:%v", err)
	}

	sql2 := "COPY pgbench_accounts_copy FROM stdin"
	query2 = (&pgproto3.Query{String: sql2}).Encode(query2)

	if err = dest.SendBytes(context.Background(), query2); err != nil {
		log.Fatalf("copy from stdin failed:%v", err)
	}

	syncChan := make(chan bool)
	errChan := make(chan *pgconn.PgError)

	go func() {
		var err error
		var msg pgproto3.BackendMessage

loop:
		for {
			msg, err = dest.ReceiveMessage(context.Background())
			if err != nil {
				log.Fatalf("copy from stdin failed:%v", err)
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyInResponse:
				log.Println("dest: copy in started");
				syncChan <- true
			case *pgproto3.CommandComplete:
				log.Println("dest: command complete");
				syncChan <- true
				break loop
			case *pgproto3.ErrorResponse:
				pgErr := ErrorResponseToPgError(msg)
				errChan <- pgErr
				log.Printf("dest: error response: %v\n", pgErr);
				break loop
			default:
				log.Printf("unexpected destination message: %+v\n", msg)
			}

			// wait for CopyDone message
			select {
				case <-syncChan:
					continue
			}
		}
	}()

	waitForCopyIn := true

	buf := make([]byte, 0, 1000000000)
	buf = append(buf, 'd')
	sp := len(buf)
	buf = buf[0 : 5]
	rowsCopied := 0
	rp := 0

loop:
	for {
		var msg pgproto3.BackendMessage
		msg, err = src.ReceiveMessage(context.Background())
		if err != nil {
			log.Fatalf("copy to stdout failed:%v", err)
		}

		if waitForCopyIn {
			<-syncChan
			waitForCopyIn = false
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyOutResponse:
			log.Println("src: copy out started");
		case *pgproto3.CopyDone:
			log.Println("src: finishing copy out");

			if err = sendCopyDone(context.Background(), dest); err != nil {
				log.Panicf("sending to destination failed:%v", err)
			}
			syncChan <- true
		case *pgproto3.CopyData:
			buf[rp] := 'd'
			rp += 1
			pgio.SetInt32(buf[rp], int32(len(msg.Data)+4))
			buf = buf[0:len(buf) + len(msg.Data) + 5]
			copy(buf[5:], msg.Data)

			if err = dest.SendBytes(context.Background(), buf); err != nil {
				log.Panicf("sending to destination failed:%v", err)
			}
			rowsCopied += 1
		case *pgproto3.ReadyForQuery:
			log.Println("src: got ready for query");
			break loop
		case *pgproto3.CommandComplete:
			log.Println("src: command complete");
		case *pgproto3.ErrorResponse:
			rowsCopied = 0
			pgErr := ErrorResponseToPgError(msg)
			log.Printf("src: got error response: %v, finishing\n", pgErr);

			if err = sendCopyFail(context.Background(), dest); err != nil {
				log.Panicf("sending to destination failed:%v", err)
			}
			syncChan <- true
		default:
			log.Printf("src msg: %+v\n", msg);
		}
	}

	log.Printf("rows copied: %d\n", rowsCopied)

	// wait for CommandComplete from dest
	<-syncChan
}

func main() {
	f, err := os.Create("cpuprofile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	manualCopy()
	var usage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	log.Printf("  CPU time: %.06f sec user, %.06f sec system\n",
           userTime(&usage), systemTime(&usage));
	log.Printf("  Max resident memory size (kb): %d\n", usage.Maxrss);
}
