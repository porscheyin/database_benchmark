/* 
 * File:   redis_benchmark.h
 * Author: Yin Zhang
 *
 * Created on April 3, 2014, 5:30 PM
 */

#ifndef REDIS_BENCHMARK_H
#define	REDIS_BENCHMARK_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include "utils.h"
#include <hiredis/hiredis.h>

bool redis_init(void);

bool redis_close(void);

bool redis_add(int iterations);

bool redis_read(int iterations);

bool redis_update(int iterations);

bool redis_delete(int iterations);

static void generate_redis_cmd(int op_type, int id, char *buf);

bool redis_clear(void);

#endif	/* REDIS_BENCHMARK_H */
