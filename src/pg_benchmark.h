/* 
 * File:   pg_benchmark.h
 * Author: Qiulin Li
 *
 * Created on April 12, 2014, 2:47 PM
 */

#ifndef PG_BENCHMARK_H
#define	PG_BENCHMARK_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <libpq-fe.h>
#include <sys/time.h>
#include <time.h>
#include "utils.h"

bool pg_clear(void);

bool pg_init(void);

void generate_sql_cmd(int op_type, int id, char *buf);

bool pg_exec_cmd(int op_type, int key, int *rows);

bool pg_add(int iterations);

bool pg_read(void);

bool pg_update(int iterations);

bool pg_delete(int iterations);

bool pg_delete_all(void);

bool pg_close(void);

#endif	/* PG_BENCHMARK_H */

