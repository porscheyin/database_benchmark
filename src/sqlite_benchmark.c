/* 
 * File:   sqlite_benchmark.c
 * Author: Yin Zhang & Fei Li
 *
 * Created on April 2, 2014, 5:18 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <time.h>
#include <sqlite3.h>
#include "sqlite_benchmark.h"
#include "utils.h"

extern int scale;
extern const int names_size;
static sqlite3 *db = NULL;
static struct timeval start, end;
static char sqlite_cmd[BUF_LEN] = {0};
static const char *db_file = "/home/yz/Programs/sqlite3/test.db";

bool sqlite_clear(void)
{
    if (db == NULL)
        if (!sqlite_init())
            return false;

    int id = 0, rows = 0;

    if (!sqlite_exec_cmd(OP_DELETE, id, &rows))
        return false;
    return true;
}

bool sqlite_init(void)
{
    int ret;
    generate_srand();

    ret = sqlite3_open(db_file, &db);
    if (ret)
    {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        return false;
    }
    return true;
}

bool sqlite_exec_cmd(int op_type, int key, int *rows)
{
    char *err_msg;
    int ret;

    generate_sql_cmd(op_type, key, sqlite_cmd);

    ret = sqlite3_exec(db, sqlite_cmd, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    *rows = sqlite3_changes(db);

    return true;
}

bool sqlite_add(int iterations)
{
    int rows = 0, row_count = 0;

    gettimeofday(&start, NULL);


    int ret;
    char *err_msg;

    ret = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }



    for (int id = 0; id < iterations; id++)
    {
        if (!sqlite_exec_cmd(OP_ADD, id, &rows))
            return false;
        row_count++;
    }

    ret = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(SQLITE, OP_ADD, row_count, get_elapsed_time(start, end) / (row_count / FORMAL_FACTOR));
    //print_benchmark_info(SQLITE, OP_ADD, row_count, get_elapsed_time(start, end));
    return true;
}

bool sqlite_read(void)
{
    int ret;
    char *errmsg;
    char **result;
    int rows, cols;
    const char *cmd = "SELECT * FROM persons";

    gettimeofday(&start, NULL);

    char *err_msg;

    ret = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    ret = sqlite3_get_table(db, cmd, &result, &rows, &cols, &errmsg);
    if (ret != SQLITE_OK)
    {
        printf("SQLite query execution failed. %d: %s\n", ret, errmsg);
        return false;
    }

    ret = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    gettimeofday(&end, NULL);
    print_benchmark_info(SQLITE, OP_READ, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    //print_benchmark_info(SQLITE, OP_READ, rows, get_elapsed_time(start, end));
    return true;
}

bool sqlite_update(int iterations)
{
    int rows = 0, row_count = 0;

    gettimeofday(&start, NULL);

    int ret;
    char *err_msg;

    ret = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    for (int id = 0; id < iterations; id++)
    {
        if (!sqlite_exec_cmd(OP_UPDATE, id, &rows))
            return false;
        row_count += rows;
    }

    ret = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    gettimeofday(&end, NULL);
    print_benchmark_info(SQLITE, OP_UPDATE, row_count, get_elapsed_time(start, end) / (row_count / FORMAL_FACTOR));
    //print_benchmark_info(SQLITE, OP_UPDATE, row_count, get_elapsed_time(start, end));
    return true;
}

bool sqlite_delete(int iterations)
{
    int rows = 0, row_count = 0;

    gettimeofday(&start, NULL);

    int ret;
    char *err_msg;

    ret = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    for (int id = 0; id < iterations; id++)
    {
        if (!sqlite_exec_cmd(OP_DELETE, id, &rows))
            return false;
        row_count += rows;
    }

    ret = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    gettimeofday(&end, NULL);
    print_benchmark_info(SQLITE, OP_DELETE, row_count, get_elapsed_time(start, end) / (row_count / FORMAL_FACTOR));
    //print_benchmark_info(SQLITE, OP_DELETE, row_count, get_elapsed_time(start, end));
    return true;
}

bool sqlite_delete_all(void)
{
    int id = 0, rows = 0;

    gettimeofday(&start, NULL);

    int ret;
    char *err_msg;

    ret = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    if (!sqlite_exec_cmd(OP_DELETE_ALL, id, &rows))
        return false;

    ret = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    
    gettimeofday(&end, NULL);
    print_benchmark_info(SQLITE, OP_DELETE, rows, get_elapsed_time(start, end) / (rows / FORMAL_FACTOR));
    //print_benchmark_info(SQLITE, OP_DELETE, rows, get_elapsed_time(start, end));
    return true;
}

bool sqlite_close(void)
{
    int ret;
    /* close the connection to the database and cleanup */
    if (db)
    {
        ret = sqlite3_close(db);
        if (ret != SQLITE_OK)
        {
            printf("SQLite close failed.");
            return false;
        }
    }
    else
        return true;
}
