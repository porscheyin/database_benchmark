/* 
 * File:   utils.c
 * Author: Yin Zhang
 *
 * Created on April 2, 2014, 4:46 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "utils.h"

char *names[] = {"Jacob", "Sophia", "Mason", "Emma", "Ethan", "Isabella", "Noah", "Olivia", "William", "Ava",
    "Liam", "Emily", "Jayden", "Abigail", "Michael", "Mia", "Alexander", "Madison", "Aiden", "Elizabeth"};

int names_size = sizeof (names) / sizeof (names[0]);

void generate_srand(void)
{
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    srand((unsigned int) tp.tv_nsec);
}

int get_rand_num(int limit)
{
    return rand() % limit;
}

char *generate_name(int size, int *idx)
{
    *idx = get_rand_num(size);
    return names[*idx];
}

char *generate_gender(int idx)
{
    if (idx % 2 == 0)
        return "M";
    else
        return "F";
}

int generate_age(int min, int max)
{
    double j;
    int i, age, range;

    i = rand();
    range = max - min;
    j = ((double) i / (double) RAND_MAX);
    i = (int) (j * (double) range);
    age = (i += min);
    return age;
}

double get_elapsed_time(struct timeval start, struct timeval end)
{
    double ret = (double) (end.tv_sec - start.tv_sec) * 1000 +
            (double) (end.tv_usec - start.tv_usec) / 1000;

    return ret;
}

char *int_to_str(int num, char *buf, int len)
{
    if (buf)
    {
        memset(buf, 0, len);
        sprintf(buf, "%d", num);
    }
    return buf;
}

void generate_sql_cmd(int op_type, int id, char *buf)
{
    switch (op_type)
    {
        case OP_ADD:
        {
            int idx;
            char *name = generate_name(names_size, &idx);
            char *gender = generate_gender(idx);
            int age = generate_age(MIN_AGE, MAX_AGE);
            sprintf(buf, "INSERT INTO persons VALUES ('%d', '%s', '%s', '%d');", id, name, gender, age);
            break;
        }
        case OP_READ:
        {
            sprintf(buf, "SELECT * FROM persons;");
            break;
        }
        case OP_UPDATE:
        {
            int idx;
            char *name = generate_name(names_size, &idx);
            char *gender = generate_gender(idx);
            int age = generate_age(MIN_AGE, MAX_AGE);
            sprintf(buf, "UPDATE persons SET name = '%s', gender = '%s', age = '%d' WHERE id = '%d';",
                    name, gender, age, id);
            break;
        }
        case OP_DELETE:
        {
            sprintf(buf, "DELETE FROM persons WHERE id = '%d';", id);
            break;
        }
        case OP_DELETE_ALL:
        {
            sprintf(buf, "DELETE FROM persons;");
            break;
        }
    }
}

void print_benchmark_info(int db_type, int op_type, int num, double time)
{
    char *db, *op;
    switch (db_type)
    {
        case BERKELEYDB:
            db = "Berkeley DB";
            break;
        case POSTGRESQL:
            db = "PostgreSQL";
            break;
        case SQLITE:
            db = "SQLite";
            break;
        case REDIS:
            db = "Redis";
            break;
    }

    switch (op_type)
    {
        case OP_ADD:
        {
            op = "Added";
            break;
        }
        case OP_READ:
        {
            op = "Read";
            break;
        }
        case OP_UPDATE:
        {
            op = "Updated";
            break;
        }
        case OP_DELETE:
        {
            op = "Deleted";
            break;
        }
        default:
            break;
    }

    //    printf("%-12s : %10.3f ms: %s %d data.\n", db, time, op, num);
    printf("%-12s : %10.3fms / %d records of data. %s %d data.\n", db, time, FORMAL_FACTOR, op, num);
}
