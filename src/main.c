/* 
 * File:   main.c
 * Author: Yin Zhang
 *
 * Created on April 2, 2014, 10:06 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include "bdb_benchmark.h"
#include "pg_benchmark.h"
#include "sqlite_benchmark.h"
#include "redis_benchmark.h"

int main(void)
{
    bool ret;
    int iterations;

    printf("Please input the number of data set: ");
    scanf("%d", &iterations);

    do
    {
        printf("\nBerkeley DB benchmark is running...\n");
        ret = bdb_clear();
        if (!ret) break;
        ret = bdb_init();
        if (!ret) break;
        ret = bdb_add(iterations);
        if (!ret) break;
        ret = bdb_read();
        if (!ret) break;
        ret = bdb_update(iterations);
        if (!ret) break;
        ret = bdb_delete();
        if (!ret) break;

        printf("\nPostgreSQL benchmark is running...\n");
        ret = pg_init();
        if (!ret) break;
        ret = pg_clear();
        if (!ret) break;
        ret = pg_add(iterations);
        if (!ret) break;
        ret = pg_read();
        if (!ret) break;
        ret = pg_update(iterations);
        if (!ret) break;
        ret = pg_delete(iterations);
        if (!ret) break;

        printf("\nSQLite benchmark is running...\n");
        ret = sqlite_init();
        if (!ret) break;
        ret = sqlite_clear();
        if (!ret) break;
        ret = sqlite_add(iterations);
        if (!ret) break;
        ret = sqlite_read();
        if (!ret) break;
        ret = sqlite_update(iterations);
        if (!ret) break;
        ret = sqlite_delete(iterations);
        if (!ret) break;

        printf("\nRedis benchmark is running...\n");
        ret = redis_init();
        if (!ret) break;
        ret = redis_clear();
        if (!ret) break;
        ret = redis_add(iterations);
        if (!ret) break;
        ret = redis_read(iterations);
        if (!ret) break;
        ret = redis_update(iterations);
        if (!ret) break;
        ret = redis_delete(iterations);
        if (!ret) break;
    }
    while (0);

    bdb_close();
    pg_close();
    sqlite_close();
    redis_close();

    if (!ret)
    {
        printf("Error occurred. Now shut down!\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
