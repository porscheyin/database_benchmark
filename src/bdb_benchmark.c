/* 
 * File:   bdb_benchmark.c
 * Author: Yin Zhang
 *
 * Created on April 2, 2014, 2:10 AM
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include "utils.h"
#include <db.h>

#define	DATABASE "access.db"
extern int scale;
extern int names_size;

bool bdb_save_file = true;

static DB *db;
static DBC *dbc;
static DBT key, data;
static struct timeval start, end;
static char bdb_data[BUF_LEN] = {0};
static char key_buf[SMALL_BUF_LEN];
static char *database = DATABASE;
static const char *progname = "bdb_benchmark"; /* Program name. */
static int buf_len = sizeof (key_buf) / sizeof (key_buf[0]);

static char *generate_new_data(void)
{
    int idx;
    char *name = generate_name(names_size, &idx);
    char *gender = generate_gender(idx);
    int age = generate_age(MIN_AGE, MAX_AGE);
    sprintf(bdb_data, "%s;%s;%d", name, gender, age);

    return bdb_data;
}

bool bdb_clear(void)
{
    if (access(database, F_OK) == 0)
    {

        if (remove(database) == 0)
            return true;
        return false;
    }
    return true;
}

bool bdb_init(void)
{
    int ret;

    generate_srand();

    /* Create and initialize database object, open the database. */
    if ((ret = db_create(&db, NULL, 0)) != 0)
    {
        fprintf(stderr, "%s: db_create: %s\n", progname, db_strerror(ret));
        goto err;
    }
    db->set_errfile(db, stderr);
    db->set_errpfx(db, progname);
    if ((ret = db->set_pagesize(db, 1024)) != 0)
    {
        db->err(db, ret, "set_bdb_pagesize");
        goto err;
    }
    if ((ret = db->set_cachesize(db, 0, 32 * 1024, 0)) != 0)
    {
        db->err(db, ret, "set_bdb_cachesize");
        goto err;
    }
    if ((ret = db->open(db, NULL, database, NULL, DB_BTREE, DB_CREATE, 0664)) != 0)
    {
        db->err(db, ret, "%s: open", database);
        goto err;
    }
    return true;
err:
    return false;
}

bool bdb_add(int iterations)
{
    int ret;
    char *key_str, *data_str;

    memset(&key, 0, sizeof (DBT));
    memset(&data, 0, sizeof (DBT));

    gettimeofday(&start, NULL);

    for (long id = 0; id < iterations; id++)
    {
        key_str = int_to_str(id, key_buf, buf_len);
        key.data = key_str;
        key.size = strlen(key_str) + 1;

        data_str = generate_new_data();
        data.data = data_str;
        data.size = strlen(data_str) + 1;
        if ((ret = db->put(db, NULL, &key, &data, DB_NOOVERWRITE)) != 0)
        {
            db->err(db, ret, "DB->put");
            if (ret != DB_KEYEXIST)
            {
                return false;
            }
        }
    }

    if (bdb_save_file)
    {
        if (ret = db->sync(db, 0) != 0)
        {
            db->err(db, ret, "DB->sync");
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(BERKELEYDB, OP_ADD, iterations,
                         get_elapsed_time(start, end) / ((iterations / FORMAL_FACTOR) < 1 ? 1 : iterations / FORMAL_FACTOR));
    return true;
}

bool bdb_read(void)
{
    int count = 0;
    int ret;

    /* Acquire a cursor for the database. */
    if ((ret = db->cursor(db, NULL, &dbc, 0)) != 0)
    {
        db->err(db, ret, "DB->cursor");
        goto err1;
    }

    /* Initialize the key/data pair so the flags aren't set. */
    memset(&key, 0, sizeof (key));
    memset(&data, 0, sizeof (data));

    gettimeofday(&start, NULL);

    /* Walk through the database and print out the key/data pairs. */
    while ((ret = dbc->get(dbc, &key, &data, DB_NEXT)) == 0)
    {
        //printf("%s : %s\n", (char *) key.data, (char *) data.data);
        count++;
    }
    if (ret != DB_NOTFOUND) // EOF.
    {
        db->err(db, ret, "DBcursor->get");
        goto err2;
    }

    if (bdb_save_file)
    {
        if (ret = db->sync(db, 0) != 0)
        {
            db->err(db, ret, "DB->sync");
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(BERKELEYDB, OP_READ, count,
                         get_elapsed_time(start, end) / ((count / FORMAL_FACTOR) < 1 ? 1 : count / FORMAL_FACTOR));

    /* Close cursor. */
    if ((ret = dbc->close(dbc)) != 0)
    {
        db->err(db, ret, "DBcursor->close");
        goto err1;
    }

    return true;

err2:
    (void) dbc->close(dbc);
err1:
    return false;
}

bool bdb_update(int size)
{
    int ret, count = 0;
    char *key_str, *data_str;

    memset(&key, 0, sizeof (DBT));
    memset(&data, 0, sizeof (DBT));

    if ((ret = db->cursor(db, NULL, &dbc, 0)) != 0)
    {
        db->err(db, ret, "DB->cursor");
        return false;
    }

    gettimeofday(&start, NULL);

    int id;
    for (id = 0; id < size; id++)
    {
        key_str = int_to_str(id, key_buf, buf_len);
        key.data = key_str;
        key.size = strlen(key_str) + 1;

        if ((ret = dbc->get(dbc, &key, &data, DB_SET)) == 0)
        {
            data_str = generate_new_data();
            data.data = data_str;
            data.size = strlen(data_str) + 1;

            if ((ret = dbc->put(dbc, &key, &data, DB_CURRENT)) == 0)
            {
                //printf("%s : %s\n", (char *) key.data, (char *) data.data);
                count++;
            }
            else
            {
                db->err(db, ret, "DBcursor->put");
                if (ret != DB_KEYEXIST)
                    return false;
            }
        }
        else
        {
            db->err(db, ret, "DBcursor->get");
            return false;
        }
    }

    if (bdb_save_file)
    {
        if (ret = db->sync(db, 0) != 0)
        {
            db->err(db, ret, "DB->sync");
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(BERKELEYDB, OP_UPDATE, count,
                         get_elapsed_time(start, end) / ((count / FORMAL_FACTOR) < 1 ? 1 : count / FORMAL_FACTOR));

    /* Close cursor. */
    if ((ret = dbc->close(dbc)) != 0)
    {
        db->err(db, ret, "DBcursor->close");
        return false;
    }

    return true;
}

bool bdb_delete(void)
{
    int idx = 0;
    int ret = 0;

    /* Acquire a cursor for the database. */
    if ((ret = db->cursor(db, NULL, &dbc, 0)) != 0)
    {
        db->err(db, ret, "DB->cursor");
        goto err1;
    }

    memset(&key, 0, sizeof (key));
    memset(&data, 0, sizeof (data));

    gettimeofday(&start, NULL);

    /* Walk through the database and print out the key/data pairs. */
    while ((ret = dbc->get(dbc, &key, &data, DB_NEXT)) == 0)
    {
        if ((ret = dbc->del(dbc, 0)) != 0)
        {
            db->err(db, ret, "DBcursor->del");
            goto err2;
        }
        idx++;
    }
    if (ret != DB_NOTFOUND) // EOF.
    {
        db->err(db, ret, "DBcursor->get");
        goto err2;
    }

    if (bdb_save_file)
    {
        if (ret = db->sync(db, 0) != 0)
        {
            db->err(db, ret, "DB->sync");
            return false;
        }
    }

    gettimeofday(&end, NULL);
    print_benchmark_info(BERKELEYDB, OP_DELETE, idx,
                         get_elapsed_time(start, end) / ((idx / FORMAL_FACTOR) < 1 ? 1 : idx / FORMAL_FACTOR));

    /* Close cursor*/
    if ((ret = dbc->close(dbc)) != 0)
    {
        db->err(db, ret, "DBcursor->close");
        goto err1;
    }

    return true;

err2:
    (void) dbc->close(dbc);
err1:
    return false;
}

bool bdb_close(void)
{
    int ret;
    if (db != NULL)
    {
        if ((ret = db->close(db, 0)) != 0)
        {
            fprintf(stderr, "%s: DB->close: %s\n", progname, db_strerror(ret));
            return false;
        }
    }
    return true;
}
