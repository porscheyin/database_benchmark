/* 
 * File:   sqlite_benchmark.h
 * Author: Fei Li
 *
 * Created on April 2, 2014, 5:14 PM
 */

#ifndef SQLITE_BENCHMARK_H
#define	SQLITE_BENCHMARK_H

bool sqlite_clear(void);

bool sqlite_init(void);

bool sqlite_exec_cmd(int op_type, int key, int *rows);

bool sqlite_add(int iterations);

bool sqlite_read(void);

bool sqlite_update(int iterations);

bool sqlite_delete(int iterations);

bool sqlite_delete_all(void);

bool sqlite_close(void);

#endif	/* SQLITE_BENCHMARK_H */

