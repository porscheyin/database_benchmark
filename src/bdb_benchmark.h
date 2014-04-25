/* 
 * File:   bdb_benchmark.h
 * Author: Yin Zhang
 *
 * Created on April 2, 2014, 2:05 AM
 */

#ifndef BDB_BENCHMARK_H
#define	BDB_BENCHMARK_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <db.h>
#include "utils.h"

#define	DATABASE "access.db"

extern int names_size;

bool bdb_clear(void);

bool bdb_init(void);

bool bdb_add(int iterations);

bool bdb_read(void);

bool bdb_update(int id);

bool bdb_delete(void);

bool bdb_close(void);

#endif	/* BDB_BENCHMARK_H */

