#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libpq-fe.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <sys/resource.h>

#define GENERAL_ERROR 1

PGconn*
openConnection(const char *conninfo)
{
    time_t  endwait;
    PGconn *conn    = NULL;
    int     timeout = 120; // in seconds
    char   *err;
    bool    is_wait = false;
    const  char *err_wait = "FATAL:  the database system is"; // the error we wait for recov


    // try to connect for timeout seconds
    endwait = time (NULL) + timeout;
    do
    {
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            err = PQerrorMessage(conn); // does not survive PQfinish
            is_wait = (strncmp(err_wait, err, strlen(err_wait)) == 0);
            fprintf(stderr, "Connection to `%s` failed:\n    %s", conninfo, err);
            PQfinish(conn);
            conn = NULL;
            if (is_wait)
               sleep(5);
        }

    }
	while(time (NULL) < endwait && PQstatus(conn) == CONNECTION_BAD && is_wait);

  return conn;
}

bool
execSQL(PGconn *conn, ExecStatusType status, const char *sql )
{
    PGresult *res = PQexec(conn, sql);
    if (PQresultStatus(res) != status)
    {
        fprintf(stderr, "%.150s failed:\n    %s\n", sql, PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    PQclear(res);

    return true;
}

static void
end_transaction(PGconn *conn, const char *cmd)
{
    PGTransactionStatusType transaction_status;
    if (conn)
    {
        transaction_status = PQtransactionStatus(conn);
        if (transaction_status != PQTRANS_IDLE && transaction_status != PQTRANS_UNKNOWN)
            execSQL(conn, PGRES_COMMAND_OK, cmd);
    }
}

static void
fail_and_exit_with_code(PGconn *src, PGconn *dest, int code)
{
    end_transaction(src, "ROLLBACK");
    end_transaction(dest, "ROLLBACK");
    PQfinish(src);
    PQfinish(dest);
    exit(code);
}

static void
fail_and_exit(PGconn *src, PGconn *dest)
{
    fail_and_exit_with_code(src, dest, GENERAL_ERROR);
}

bool copy() {
    PGconn        *src, *dest;
    PGresult      *res;
    char          *buffer;
    int            num_copy   = 0;
    int            copy_res;

    src  = openConnection("dbname=test");
    dest = openConnection("dbname=test");

    if (!execSQL(src,
                  PGRES_COPY_OUT,
                  "COPY (SELECT * FROM ONLY pgbench_accounts) TO STDOUT;"))
        fail_and_exit(src, dest);

    if (!execSQL(dest, PGRES_COPY_IN, "COPY pgbench_accounts_copy FROM STDIN"))
        fail_and_exit(src, dest);

    do
    {
        int nBytes = PQgetCopyData(src, &buffer, 0);

        if (nBytes > 0)
        {
            num_copy++;

            copy_res = PQputCopyData(dest, buffer, nBytes);

            if (copy_res == 1)
                continue;

            if (copy_res == -1)
            {
                fprintf(stderr, "Error sending rows: %s", PQerrorMessage(dest));
                PQfreemem(buffer);
                PQputCopyEnd(dest, "reshard copy failed");
                fail_and_exit(src, dest);
            }
        }

        PQfreemem(buffer);

        if (nBytes == -1)
            break;

        if (nBytes == -2)
        {
            fprintf(stderr, "Error receiving rows: %s", PQerrorMessage(src));
            PQputCopyEnd(dest, "reshard copy failed");
            fail_and_exit(src, dest);
        }

        if (false)
        {
            PQputCopyEnd(dest, "resharding interrupted by user");
            fail_and_exit(src, dest);
        }

    } while (1);

    copy_res = PQputCopyEnd(dest, NULL);

    if (copy_res == 1)
    {
        res = PQgetResult(dest);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "ERROR %s\n", PQerrorMessage(dest));
            PQclear(res);
            fail_and_exit(src, dest);
        }

        PQclear(res);
    }
	printf("done\n");
}

float userTime(struct rusage *t)
{
    return t->ru_utime.tv_sec + 1e-6*(t->ru_utime.tv_usec);
}

float systemTime(struct rusage *t)
{
    return t->ru_stime.tv_sec + 1e-6*(t->ru_stime.tv_usec);
}

int main() {
	copy();

	int who = RUSAGE_SELF;
    struct rusage usage;
    int ret;
	ret = getrusage(who, &usage);
	printf("  CPU time: %.06f sec user, %.06f sec system\n",
           userTime(&usage), systemTime(&usage));
	printf("  Max resident memory size (kb): %d\n", usage.ru_maxrss);

	return 0;
}
