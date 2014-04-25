/* 
 * File:   pg_benchmark.h
 * Author: Qiulin Li
 *
 * Created on April 10, 2014, 9:23 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <libpq-fe.h>
#include <sys/time.h>
#include <time.h>
#include "pg_benchmark.h"
#include "utils.h"

extern int scale;
extern const int names_size;
static PGconn *pgconn = NULL;
static PGresult *result;
static struct timeval start, end;
static char pg_sql_cmd[BUF_LEN] = {0};
static const char *conn_str = "host=localhost port=5432 dbname=mydb user=postgres password=123456";

bool pg_clear(void)
{
    if (pgconn == NULL)
        if (!pg_init())
            return false;

    int id = 0, rows = 0;

    if (!pg_exec_cmd(OP_DELETE, id, &rows))
        return false;
    return true;
}

bool pg_init(void)
{
    generate_srand();

    /* Make a connection to the database */
    pgconn = PQconnectdb(conn_str);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(pgconn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(pgconn));
        PQfinish(pgconn);
        return false;
    }
    return true;
}

static bool pg_exec_txn(char *cmd)
{
    ExecStatusType query_status;

    result = PQexec(pgconn, cmd);
    query_status = PQresultStatus(result);
    if (query_status != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "PostgreSQL execute transaction command failed: %s", PQerrorMessage(pgconn));
        PQclear(result);
        return false;
    }
    PQclear(result);
    return true;
}

bool pg_exec_cmd(int op_type, int key, int *rows)
{
    ExecStatusType query_status;

    generate_sql_cmd(op_type, key, pg_sql_cmd);
    result = PQexec(pgconn, pg_sql_cmd);
    query_status = PQresultStatus(result);

    if (query_status == PGRES_COMMAND_OK || query_status == PGRES_TUPLES_OK)
    {
        *rows += atoi(PQcmdTuples(result));
    }
    else
    {
        fprintf(stderr, "PostgreSQL execute command failed: %s", PQerrorMessage(pgconn));
        PQclear(result);
        return false;
    }
    PQclear(result);
    return true;
}

bool pg_add(int iterations)
{
    int rows = 0;

    gettimeofday(&start, NULL);

    if (!pg_exec_txn("BEGIN;"))
        return false;

    for (int id = 0; id < iterations; id++)
    {
        if (!pg_exec_cmd(OP_ADD, id, &rows))
            return false;
    }

    if (!pg_exec_txn("COMMIT;"))
        return false;

    gettimeofday(&end, NULL);
    print_benchmark_info(POSTGRESQL, OP_ADD, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    return true;
}

bool pg_read(void)
{
    int id = 0, rows = 0;

    gettimeofday(&start, NULL);

    if (!pg_exec_cmd(OP_READ, id, &rows))
        return false;

    gettimeofday(&end, NULL);
    print_benchmark_info(POSTGRESQL, OP_READ, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    return true;
}

bool pg_update(int iterations)
{
    int rows = 0;

    gettimeofday(&start, NULL);

    if (!pg_exec_txn("BEGIN;"))
        return false;

    for (int id = 0; id < iterations; id++)
    {
        if (!pg_exec_cmd(OP_UPDATE, id, &rows))
            return false;
    }

    if (!pg_exec_txn("COMMIT;"))
        return false;

    gettimeofday(&end, NULL);
    print_benchmark_info(POSTGRESQL, OP_UPDATE, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    //print_benchmark_info(POSTGRESQL, OP_UPDATE, rows, get_elapsed_time(start, end));
    return true;
}

bool pg_delete(int iterations)
{
    int rows = 0;

    gettimeofday(&start, NULL);

    if (!pg_exec_txn("BEGIN;"))
        return false;

    for (int id = 0; id < iterations; id++)
    {
        if (!pg_exec_cmd(OP_DELETE, id, &rows))
            return false;
    }

    if (!pg_exec_txn("COMMIT;"))
        return false;

    gettimeofday(&end, NULL);
    print_benchmark_info(POSTGRESQL, OP_DELETE, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    //print_benchmark_info(POSTGRESQL, OP_DELETE, rows, get_elapsed_time(start, end));
    return true;
}

bool pg_delete_all(void)
{
    int id = 0, rows = 0;

    gettimeofday(&start, NULL);

    if (!pg_exec_txn("BEGIN;"))
        return false;

    if (!pg_exec_cmd(OP_DELETE_ALL, id, &rows))
        return false;

    if (!pg_exec_txn("COMMIT;"))
        return false;

    gettimeofday(&end, NULL);
    print_benchmark_info(POSTGRESQL, OP_DELETE, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    return true;
}

bool pg_close(void)
{
    /* close the connection to the database and cleanup */
    if (pgconn)
    {
        PQfinish(pgconn);
    }
    return true;
}
