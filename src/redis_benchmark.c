/* 
 * File:   redis_benchmark.c
 * Author: Yin Zhang
 *
 * Created on April 3, 2014, 5:36 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include "utils.h"
#include "redis_benchmark.h"
#include <hiredis/hiredis.h>

redisContext *conn;
redisReply *reply;

extern int names_size;
static struct timeval start, end;
static char redis_buf[BUF_LEN] = {0};
static int buf_len = sizeof (redis_buf) / sizeof (redis_buf[0]);

bool redis_init(void)
{
    int port = 6379;
    const char *hostname = "127.0.0.1";

    struct timeval timeout = {1, 500000}; // 1.5 seconds
    conn = redisConnectWithTimeout(hostname, port, timeout);
    if (conn == NULL || conn->err)
    {
        if (conn)
        {
            printf("Connection error: %s\n", conn->errstr);
            return false;
        }
        else
        {
            printf("Connection error: can't allocate redis context\n");
            return false;
        }
    }

    return true;
}

bool redis_close(void)
{
    if (conn)
    {
        redisFree(conn);
    }
    return true;
}

bool redis_add(int iterations)
{
    char *key_str;

    gettimeofday(&start, NULL);

    for (long id = 0; id < iterations; id++)
    {
        key_str = int_to_str(id, redis_buf, buf_len);
        generate_redis_cmd(OP_ADD, id, redis_buf);
        reply = redisCommand(conn, redis_buf);
        if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str, "OK") == 0))
        {
            printf("Redis: failed to execute command[%s].\n", redis_buf);
            freeReplyObject(reply);
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(REDIS, OP_ADD, iterations,
                         get_elapsed_time(start, end) / ((iterations / FORMAL_FACTOR) < 1 ? 1 : iterations / FORMAL_FACTOR));

    return true;
}

bool redis_read(int iterations)
{
    char *key_str;

    gettimeofday(&start, NULL);

    for (long id = 0; id < iterations; id++)
    {
        key_str = int_to_str(id, redis_buf, buf_len);
        generate_redis_cmd(OP_READ, id, redis_buf);
        reply = redisCommand(conn, redis_buf);
        if (reply->type != REDIS_REPLY_STRING)
        {
            printf("Redis: failed to execute command[%s].\n", redis_buf);
            freeReplyObject(reply);
            return false;
        }
    }
    gettimeofday(&end, NULL);
    print_benchmark_info(REDIS, OP_READ, iterations,
                         get_elapsed_time(start, end) / ((iterations / FORMAL_FACTOR) < 1 ? 1 : iterations / FORMAL_FACTOR));
    freeReplyObject(reply);
    return true;
}

bool redis_update(int iterations)
{
    char *key_str;

    gettimeofday(&start, NULL);

    for (long id = 0; id < iterations; id++)
    {
        key_str = int_to_str(id, redis_buf, buf_len);
        generate_redis_cmd(OP_UPDATE, id, redis_buf);
        reply = redisCommand(conn, redis_buf);
        if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str, "OK") == 0))
        {
            printf("Redis: failed to execute command[%s].\n", redis_buf);
            freeReplyObject(reply);
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(REDIS, OP_UPDATE, iterations,
                         get_elapsed_time(start, end) / ((iterations / FORMAL_FACTOR) < 1 ? 1 : iterations / FORMAL_FACTOR));
    freeReplyObject(reply);
    return true;
}

bool redis_delete(int iterations)
{
    char *key_str;

    gettimeofday(&start, NULL);

    for (long id = 0; id < iterations; id++)
    {
        key_str = int_to_str(id, redis_buf, buf_len);
        generate_redis_cmd(OP_DELETE, id, redis_buf);
        reply = redisCommand(conn, redis_buf);

        if (reply->type != REDIS_REPLY_INTEGER || reply->integer == 0)
        {
            printf("Redis: failed to execute command[%s].\n", redis_buf);
            freeReplyObject(reply);
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(REDIS, OP_DELETE, iterations,
                         get_elapsed_time(start, end) / ((iterations / FORMAL_FACTOR) < 1 ? 1 : iterations / FORMAL_FACTOR));
    freeReplyObject(reply);
    return true;
}

static void generate_redis_cmd(int op_type, int id, char *buf)
{
    switch (op_type)
    {
        case OP_UPDATE:
        case OP_ADD:
        {
            int idx;
            char *name = generate_name(names_size, &idx);
            char *gender = generate_gender(idx);
            int age = generate_age(MIN_AGE, MAX_AGE);
            sprintf(buf, "SET %d %s;%s;%d", id, name, gender, age);
            break;
        }
        case OP_READ:
        {
            sprintf(buf, "GET %d", id);
            break;
        }
        case OP_DELETE:
        {
            sprintf(buf, "DEL %d", id);
            break;
        }
        case OP_CLEAR:
        {
            sprintf(buf, "FLUSHDB");
            break;
        }
    }
}

bool redis_clear(void)
{
    int id;

    generate_redis_cmd(OP_CLEAR, id, redis_buf);
    reply = redisCommand(conn, redis_buf);
    if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str, "OK") == 0))
    {
        printf("Redis: failed to execute command[%s].\n", redis_buf);
        freeReplyObject(reply);
        return false;
    }

    freeReplyObject(reply);
    return true;
}
