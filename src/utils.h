/* 
 * File:   utils.h
 * Author: Yin Zhang
 *
 * Created on April 2, 2014, 4:46 PM
 */

#ifndef UTILS_H
#define	UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_AGE 80
#define MIN_AGE 18
#define BUF_LEN 512
#define SMALL_BUF_LEN 32

#define OP_ADD        0
#define OP_READ       1
#define OP_UPDATE     2
#define OP_DELETE     3
#define OP_DELETE_ALL 4
#define OP_CLEAR      5

#define BERKELEYDB 0
#define POSTGRESQL 1
#define SQLITE     2
#define REDIS      3

#define FORMAL_FACTOR 1000

void generate_srand(void);

int get_rand_num(int limit);

char *generate_name(int size, int *idx);

char *generate_gender(int idx);

int generate_age(int min, int max);

char *int_to_str(int num, char *buf, int len);

void generate_sql_cmd(int op_type, int id, char *buf);

double get_elapsed_time(struct timeval start, struct timeval end);

void print_benchmark_info(int db_type, int op_type, int num, double time);

#endif	/* UTILS_H */

