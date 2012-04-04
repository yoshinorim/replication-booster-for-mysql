/**
 *   Replication Booster -- A Tool for Prefetching MySQL Slave Relay Logs
 *   Copyright (C) 2011 DeNA Co.,Ltd.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc.,
 *   51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
**/

#ifndef replication_booster_h
#define replication_booster_h

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <cstdlib>
#include <queue>
#include <binlog_api.h>
#include <mysql.h>
#include "options.h"

using mysql::Binary_log;
using mysql::Binary_log_event;
using mysql::system::create_transport;
using mysql::system::Binlog_file_driver;
using mysql::system::Binary_log_driver;

#ifdef DEBUG
#define  DBUG_PRINT(format, ...) print_log(format, ##__VA_ARGS__)
#else
#define  DBUG_PRINT(format, ...)
#endif

extern const char *VER;
extern Binary_log *binlog;
extern Binlog_file_driver *driver;
extern uint my_server_id;
extern char *relay_log_info_path;
extern char *data_dir;
extern char *sql_thread_relay_log_path;
extern uint64_t sql_thread_pos;
extern uint32_t sql_thread_timestamp;
extern pthread_mutex_t worker_mutex;
extern pthread_mutex_t relay_log_pos_mutex;
extern bool shutdown_program;

class query_queue;
extern query_queue **queue;

extern uint64_t stat_parsed_binlog_events;
extern uint64_t stat_skipped_binlog_events;
extern uint64_t stat_reached_ahead_relay_log;
extern uint64_t stat_reached_end_of_relay_log;
extern uint64_t stat_unrelated_binlog_events;
extern uint64_t stat_discarded_in_front_queries;
extern uint64_t stat_pushed_queries;
extern uint64_t stat_popped_queries;
extern uint64_t stat_old_queries;
extern uint64_t stat_discarded_queries;
extern uint64_t stat_converted_queries;
extern uint64_t stat_executed_selects;
extern uint64_t stat_error_selects;

enum relay_log_info_type { RLI_TYPE_FILE= 0, RLI_TYPE_TABLE= 1, };
enum relay_log_code { READING= 0, END_OF_FILE= 1, TIMESTAMP_LIMIT= 2, };

typedef struct query
{
  const mysql::Query_event *qev;
  uint64_t pos;
  bool shutdown;
} query_t;

typedef struct status
{
  enum relay_log_code code;
  const char *next_file;
  uint64_t current_pos;
  uint64_t next_pos;
  int event_type;
  bool got_rotate_event;
} status_t;

typedef struct worker_info
{
  pthread_t ptid;
  uint worker_id;
} worker_info_t;

void *prefetch_worker(void *worker_info);
void print_log(const char *format, ...);
void print_log(const std::string &str);
void free_query(query_t *query, char *select = NULL);
int check_local(const char *hostname_or_ip);

class query_queue
{
private:
  std::queue<query_t*> queue;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool empty() const
  {
    return queue.empty();
  }

public:
  query_queue()
  {
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
  }
  ~query_queue()
  {
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&cond);
  }

  uint get_size()
  {
    uint size;
    pthread_mutex_lock(&mutex);
    size= queue.size();
    pthread_mutex_unlock(&mutex);
    return size;
  }

  void push(query_t *data)
  {
    pthread_mutex_lock(&mutex);
    queue.push(data);
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
  }

  bool try_pop(query_t *popped_value)
  {
    pthread_mutex_lock(&mutex);
    if(queue.empty())
    {
      return false;
    }
    popped_value=queue.front();
    queue.pop();
    pthread_mutex_unlock(&mutex);
    return true;
  }

  query_t *wait_and_pop()
  {
    pthread_mutex_lock(&mutex);
    while(queue.empty())
    {
      pthread_cond_wait(&cond, &mutex);
    }
    query_t* popped_value=queue.front();
    queue.pop();
    pthread_mutex_unlock(&mutex);
    return popped_value;
  }

  void clear()
  {
    std::queue<query_t*> empty;
    pthread_mutex_lock(&mutex);
    std::swap(queue, empty);
    pthread_mutex_unlock(&mutex);
    while(!empty.empty())
    {
      query_t* q=empty.front();
      empty.pop();
      free_query(q);
    }
  }
};

#endif
