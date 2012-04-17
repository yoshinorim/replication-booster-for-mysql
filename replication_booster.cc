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

#include "replication_booster.h"
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <libgen.h>

const char *VER= "0.2";
query_queue **queue;
Binary_log *binlog;
Binlog_file_driver *driver;
uint my_server_id;
char *url_for_binlog_api;
char *sql_thread_relay_log_path;
uint32_t sql_thread_timestamp;
uint64_t sql_thread_pos;
pthread_mutex_t worker_mutex;
pthread_mutex_t relay_log_pos_mutex;
enum relay_log_info_type rli_type= RLI_TYPE_FILE;
bool shutdown_program= false;
unsigned long prefetch_position= 0;
uint32_t prefetch_timestamp= 0;
bool is_sql_thread_running= true;

uint64_t stat_parsed_binlog_events= 0;
uint64_t stat_skipped_binlog_events= 0;
uint64_t stat_reached_ahead_relay_log= 0;
uint64_t stat_reached_end_of_relay_log= 0;
uint64_t stat_unrelated_binlog_events= 0;
uint64_t stat_discarded_in_front_queries= 0;
uint64_t stat_pushed_queries= 0;

struct timeval t_begin, t_end;
pthread_t *worker_thread_ids;
pthread_t rli_reader_thread_id;
pthread_t status_thread_id;

char *relay_log_info_path;
char *data_dir;

std::string dir_name_status_file;

void print_log(const char *format, ...)
{
  struct timeval tv;
  time_t tt;
  struct tm *tm;
  gettimeofday(&tv, 0);
  tt= tv.tv_sec;
  tm= localtime(&tt);
  fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d: ",
          tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
          tm->tm_hour, tm->tm_min, tm->tm_sec);

  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");
}

void print_log(const std::string &str)
{
  print_log("%s", str.c_str());
}

void free_query(query_t *query, char *select)
{
  if(select != NULL)
    delete[] select;
  delete query->qev;
  delete query;
}

static double timediff(struct timeval tv0, struct timeval tv1)
{
  return (tv1.tv_sec - tv0.tv_sec) + (tv1.tv_usec*1e-6 - tv0.tv_usec*1e-6);
}

static int connect_binlog_file(Binary_log *binlog)
{
  int rc;
  int retry=10;
  do
  {
    rc= binlog->connect();
    if(!rc)
      break;
    DBUG_PRINT("Failed to open binlog %s, rc=%d. retrying..", sql_thread_relay_log_path, rc);
    usleep(1000);
  } while(rc && --retry > 0);
  return rc;
}

static bool is_convert_candidate(const char *query)
{
  bool convert_candidate= true;

  /* non candidate queries: BEGIN, COMMIT/CREATE, INSERT */;
  switch (query[0])
  {
    case 'B':
    case 'b':
    case 'C':
    case 'c':
    case 'I':
    case 'i':
      convert_candidate= false;
      DBUG_PRINT("Matched non-convert query: %s", query);
      break;
  }
  return convert_candidate;
}

static status_t *read_binlog(Binary_log *binlog, int start_pos, bool init = false)
{
  int rc;
  bool eof= false;
  bool start= true;
  uint events_handled= 0;
  rc= connect_binlog_file(binlog);
  if (rc)
  {
    print_log("ERROR: Failed to open binlog file!");
    return NULL;
  }
  if (init)
  {
    binlog->set_position(4);
    Binary_log_event *e;
    rc = binlog->wait_for_next_event(&e);
    my_server_id= e->header()->server_id;
    delete e;
    DBUG_PRINT("Got server id %d", my_server_id);
  }

  status_t *status= new status_t;
  memset(status, 0, sizeof(status_t));
  status->next_pos= start_pos;
  status->event_type= 0;

  DBUG_PRINT("Set position %d", start_pos);
  binlog->set_position(start_pos);

  while (1)
  {
    if (shutdown_program || !is_sql_thread_running)
    {
      return status;
    }
    bool delete_event= true;
    Binary_log_event *event;
    prefetch_position= binlog->get_position();
    rc = binlog->wait_for_next_event(&event);
    if (rc == ERR_EOF)
    {
      eof= true;
      stat_reached_end_of_relay_log++;
      goto end;
    }
    status->code= READING;
    status->got_rotate_event= false;
    uint32_t timestamp= prefetch_timestamp= event->header()->timestamp;
    int event_length= event->header()->event_length;
    status->current_pos= status->next_pos;
    status->next_pos= status->next_pos + event_length;
    status->event_type= event->header()->type_code;
    stat_parsed_binlog_events++;
    DBUG_PRINT("Event type: %s length: %d current pos: %d next pos: %d timestamp: %d",
               mysql::system::get_event_type_str(event->get_event_type()), event_length,
               status->current_pos, status->next_pos, timestamp);

    if (start)
    {
      sql_thread_timestamp= timestamp;
      start= false;
    }

    if (timestamp >= sql_thread_timestamp + opt_read_ahead_seconds)
    {
      DBUG_PRINT("Reached end timestamp: %d, sql thread timestamp: %d",
                 timestamp, sql_thread_timestamp);
      stat_reached_ahead_relay_log++;
      usleep(opt_sleep_millis_at_read_limit);
      delete event;
      goto end;
    }

    events_handled++;
    if (events_handled <= opt_skip_events)
    {
      stat_skipped_binlog_events++;
      delete event;
      continue;
    }

    switch (status->event_type)
    {
    case mysql::QUERY_EVENT:
      {
        const mysql::Query_event *qev= static_cast<const mysql::Query_event *>(event);
        DBUG_PRINT("query= %s db= %s", qev->query.c_str(), qev->db_name.c_str());
        if (!is_convert_candidate(qev->query.c_str()))
        {
          stat_discarded_in_front_queries++;
          break;
        }

        query_t *query= new query_t;
        memset(query, 0, sizeof(query_t));
        query->qev= qev;
        query->pos= status->current_pos;
        queue[stat_pushed_queries % opt_workers]->push(query);
        stat_pushed_queries++;
        delete_event= false;
      }
      break;
    case mysql::ROTATE_EVENT:
      {
        if (event->header()->server_id == my_server_id)
        {
          mysql::Rotate_event *rot= static_cast<mysql::Rotate_event *>(event);
          status->got_rotate_event= true;
          status->next_file= rot->binlog_file.c_str();
          status->next_pos= rot->binlog_pos;
          DBUG_PRINT("filename= %s pos=%lu\n", rot->binlog_file.c_str(), rot->binlog_pos);
        }
      }
      break;
    default:
      stat_unrelated_binlog_events++;
      break;
    }
    if (delete_event)
      delete event;
  }
end:
  DBUG_PRINT("Disconnecting binlog");
  driver->disconnect();
  if (eof)
  {
    status->code= END_OF_FILE;
#ifdef DEBUG
    sleep(1);
#endif
    usleep(100);
  }
  return status;

}

static void get_file_url(const char *binlog_file_path, char **url)
{
  sprintf(*url, "file://%s", binlog_file_path);
}

static void delete_binlog_driver()
{
  if (driver != NULL)
    delete driver;
  if (binlog != NULL)
    delete binlog;
}

static void init_binlog_driver(const char *url)
{
  DBUG_PRINT("Initializing Binary_log instance, url %s", url);
  driver= (Binlog_file_driver*)create_transport(url);
  binlog= new Binary_log(driver);
}

static void init_binlog_driver(const char* binlog_file_path, char **url)
{
  get_file_url(binlog_file_path, url);
  return init_binlog_driver(*url);
}

static void read_current_relay_info()
{
  char row[PATH_MAX+1];
  char *pos;
  FILE *fp;
  fp= fopen(relay_log_info_path, "r");
  if (!fp)
  {
    print_log("ERROR: Failed to open %s, %d %s",
              relay_log_info_path, errno, strerror(errno));
    sleep(100);
    exit(1);
  }
  while (fgets(row, sizeof(row), fp) != NULL)
  {
    bool found= false;
    row[strlen(row) -1] = '\0';
    pos= row;
    // relay log file
    if (row[0] == '.')
    {
      found= true;
      pos+= 2;
      pthread_mutex_lock(&relay_log_pos_mutex);
      sprintf(sql_thread_relay_log_path, "%s/%s", data_dir, pos);
    } else if(row[0] == '/')
    {
      found= true;
      pthread_mutex_lock(&relay_log_pos_mutex);
      sprintf(sql_thread_relay_log_path, "%s", pos);
    }
    if (found)
    {
      // relay log pos
      fgets(row, sizeof(row), fp);
      row[strlen(row) -1] = '\0';
      char *x;
      sql_thread_pos= strtoull(row, &x, 0);
      pthread_mutex_unlock(&relay_log_pos_mutex);
      break;
    }
  }
  fclose(fp);
}

static void init_relay_log_info_path(MYSQL *mysql, uint version)
{
  int rc;
  char *pos;
  MYSQL_RES   *result;
  MYSQL_ROW    row;
  char relay_log_info_name[PATH_MAX+1];
  char buf[PATH_MAX+1];
  if (version > 50100)
  {
    rc= mysql_query(mysql, "SELECT @@global.relay_log_info_file AS Value");
    result = mysql_store_result(mysql);
    row = mysql_fetch_row(result);
    strcpy(relay_log_info_name, row[0]);
    mysql_free_result(result);
  } else
  {
    strcpy(relay_log_info_name, "relay-log.info");
  }

  if (relay_log_info_name[0] != '/')
  {
    pos= relay_log_info_name;
    if (relay_log_info_name[0] == '.')
      pos= pos+2;
    sprintf(buf, "%s/%s", data_dir, pos);
  } else
  {
    sprintf(buf, relay_log_info_name);
  }
  relay_log_info_path= new char[strlen(buf)+1];
  strcpy(relay_log_info_path, buf);
}

static MYSQL* init_mysql_config()
{
  MYSQL *mysql;
  MYSQL_RES   *result;
  MYSQL_ROW    row;
  int rc;
  uint version;
  char *buf;
  my_bool reconnect= true;
  if (opt_slave_socket)
    opt_slave_host = NULL;

  if (opt_slave_user && !opt_admin_user)
    opt_admin_user= opt_slave_user;

  if (opt_slave_password && !opt_admin_password)
    opt_admin_password= opt_slave_password;

  mysql= mysql_init(NULL);
  if (!mysql)
  {
    print_log("ERROR: mysql_init failed.");
    goto err;
  }
  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "client");
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  if ( !mysql_real_connect(mysql, opt_slave_host, opt_admin_user,
                           opt_admin_password, NULL, opt_slave_port,
                           opt_slave_socket, 0) )
  {
    print_log("ERROR: Failed to connect to MySQL: %d, %s",
              mysql_errno(mysql),mysql_error(mysql));
    goto err;
  }

  rc= mysql_query(mysql, "SELECT @@global.datadir AS Value");
  result = mysql_store_result(mysql);
  row = mysql_fetch_row(result);
  buf= row[0];
  if (buf[strlen(buf)-1] == '/')
    buf[strlen(buf)-1] = '\0';
  data_dir= new char[strlen(buf)+1];
  strcpy(data_dir, buf);
  mysql_free_result(result);

  version= mysql_get_server_version(mysql);
  if (version > 50600)
  {
    // TODO: supporting table type relay log
    rli_type= RLI_TYPE_FILE;
  }

  if (rli_type == RLI_TYPE_FILE)
  {
    init_relay_log_info_path(mysql, version);
  }
  return mysql;
err:
  if (mysql)
  {
    mysql_close(mysql);
  }
  return NULL;
}

extern "C" {
  static void set_shutdown(int);
}

static void set_shutdown(int)
{
  shutdown_program= true;
}

static void init_signals()
{
  int signals[] = {SIGINT,SIGILL,SIGFPE,SIGSEGV,SIGTERM,SIGABRT};
  for (uint i=0 ; i < sizeof(signals)/sizeof(int) ; i++)
    signal(signals[i], set_shutdown);
  return;
}

static inline const char *bool_to_str(bool v)
{
  return v ? "true" : "false";
}

static void print_status(FILE *stream)
{
  fprintf(stream, "Status:\n");
  pthread_mutex_lock(&relay_log_pos_mutex);
  fprintf(stream, "  Relay log file: %s\n", sql_thread_relay_log_path);
  fprintf(stream, "  Relay log (SQL thread) position: %lu\n", sql_thread_pos);
  pthread_mutex_unlock(&relay_log_pos_mutex);
  fprintf(stream, "  SQL thread timestamp: %u\n", sql_thread_timestamp);
  fprintf(stream, "  Prefetch event timestamp: %u\n", prefetch_timestamp);
  fprintf(stream, "  Prefetch event position: %lu\n", prefetch_position);
  fprintf(stream, "  Is SQL thread running: %s\n",
          bool_to_str(is_sql_thread_running));
    fprintf(stream, "  Shutdown program: %s\n", bool_to_str(shutdown_program));
}

static void print_statistics(FILE *stream)
{
  uint64_t popped_queries, old_queries, discarded_queries;
  uint64_t converted_queries, executed_selects, error_selects;

  pthread_mutex_lock(&worker_mutex);
  popped_queries = stat_popped_queries;
  old_queries = stat_old_queries;
  discarded_queries = stat_discarded_queries;
  converted_queries = stat_converted_queries;
  executed_selects = stat_executed_selects;
  error_selects = stat_error_selects;
  pthread_mutex_unlock(&worker_mutex);

  fprintf(stream, "Statistics:\n");
  fprintf(stream, " Parsed binlog events: %lu\n", stat_parsed_binlog_events);
  fprintf(stream, " Skipped binlog events by offset: %lu\n", stat_skipped_binlog_events);
  fprintf(stream, " Unrelated binlog events: %lu\n", stat_unrelated_binlog_events);
  fprintf(stream, " Queries discarded in front: %lu\n", stat_discarded_in_front_queries);
  fprintf(stream, " Queries pushed to workers: %lu\n", stat_pushed_queries);
  fprintf(stream, " Queries popped by workers: %lu\n", popped_queries);
  fprintf(stream, " Old queries popped by workers: %lu\n", old_queries);
  fprintf(stream, " Queries discarded by workers: %lu\n", discarded_queries);
  fprintf(stream, " Queries converted to select: %lu\n", converted_queries);
  fprintf(stream, " Executed SELECT queries: %lu\n", executed_selects);
  fprintf(stream, " Error SELECT queries: %lu\n", error_selects);
  fprintf(stream, " Number of times to read relay log limit: %lu\n", stat_reached_ahead_relay_log);
  fprintf(stream, " Number of times to reach end of relay log: %lu\n", stat_reached_end_of_relay_log);
}

static bool make_status_file(int *error)
{
  int fd = -1;
  bool rv = true;
  FILE *fp = NULL;
  char *status_file = NULL;
  std::string template_path;

  template_path += dir_name_status_file;
  template_path += "/replication_booster.XXXXXX";

  if (! (status_file = strdup(template_path.c_str())))
    goto err;

  if ((fd = mkstemp(status_file)) < 0)
    goto err;

  if (! (fp = fdopen(fd, "w")))
    goto err;

  print_status(fp);
  print_statistics(fp);
  fflush(fp);

  rv = rename(status_file, opt_status_file);

err:
  *error= errno;

  free(status_file);

  if (fp)
    fclose(fp);
  else if (fd > -1)
    close(fd);

  return rv;
}

static void* status_thread(void*)
{
  int state, error;

  while (opt_status_update_freq)
  {
    sleep(opt_status_update_freq);
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &state);
    if (make_status_file(&error))
      print_log("ERROR: Could not print to status file (%d)", error);
    pthread_setcancelstate(state, NULL);
  }

  return NULL;
}

static void do_shutdown()
{
  gettimeofday(&t_end, 0);
  print_log("Stopping Replication Booster..");
  delete_binlog_driver();
  for (uint i=0; i < opt_workers; i++)
  {
    query_t *query= new query_t;
    memset(query, 0, sizeof(query_t));
    query->shutdown= true;
    queue[i]->push(query);
  }
  for (uint i=0; i < opt_workers; i++)
  {
    pthread_join(worker_thread_ids[i], NULL);
    queue[i]->clear();
    delete queue[i];
  }
  pthread_join(rli_reader_thread_id, NULL);
  pthread_cancel(status_thread_id);
  pthread_join(status_thread_id, NULL);
  double total_time= timediff(t_begin,t_end);
  printf("Running duration: %10.3f seconds\n", total_time);
  print_statistics(stdout);
  mysql_library_end();
  delete[] data_dir;
  delete[] relay_log_info_path;
  delete[] url_for_binlog_api;
  delete[] sql_thread_relay_log_path;
  delete[] worker_thread_ids;
  delete[] queue;
  pthread_mutex_destroy(&worker_mutex);
  pthread_mutex_destroy(&relay_log_pos_mutex);
}

static void* rli_reader_thread(void* arg)
{
  int rc;
  uint counter= 0;
  int num_fields;
  MYSQL *mysql= (MYSQL*)arg;
  MYSQL_RES   *result;
  MYSQL_ROW    row;
  MYSQL_FIELD *field;

  while (1)
  {
    if (rli_type == RLI_TYPE_FILE)
    {
      read_current_relay_info();
    }
    if (shutdown_program)
    {
      goto end;
    }
    usleep(10000);
    counter++;
    /* checking every 2000 milliseconds */
    if (counter % 200 == 0)
    {
      rc= mysql_query(mysql, "SHOW SLAVE STATUS");
      if (rc)
      {
        print_log("ERROR: Could not execute SHOW SLAVE STATUS: %d %s",
                  mysql_errno(mysql),mysql_error(mysql));
        shutdown_program= true;
        goto end;
      }
      result = mysql_store_result(mysql);
      num_fields = mysql_num_fields(result);
      while ((row = mysql_fetch_row(result)))
      {
        int i= 0;
        while((field = mysql_fetch_field(result))!= NULL) {
          if (!strcmp(field->name, "Slave_SQL_Running"))
            break;
          i++;
        }
        if (strcmp(row[i], "Yes") && is_sql_thread_running)
        {
          print_log("WARN: SQL Thread is not running! "
                    "Sleeping until SQL Thread starts. "
                    "Check configurations for details.");
          is_sql_thread_running= false;
        } else if (!strcmp(row[i], "Yes") && !is_sql_thread_running)
        {
          print_log("SQL Thread started again. Starting slave prefetching.");
          is_sql_thread_running= true;
        }
      }
      mysql_free_result(result);
    }
  }
end:
  if(mysql)
    mysql_close(mysql);
  print_log("Terminating slave monitoring thread.");
  pthread_exit(0);
}

int main(int argc, char **argv)
{
  uint64_t pos;
  bool init=true;
  MYSQL *mysql;

  get_options(argc, argv);
  if (check_local(opt_slave_host))
  {
    goto err;
  }
  if (opt_admin_user == NULL)
    opt_admin_user= opt_slave_user;
  if (opt_admin_password == NULL)
    opt_admin_password= opt_admin_password;
  if (mysql_library_init(0, NULL, NULL)) {
    print_log("Could not initialize MySQL library.");
    exit(1);
  }
  init_signals();
  mysql= init_mysql_config();
  if (!mysql)
  {
    goto err;
  }
  pthread_mutex_init(&worker_mutex, NULL);
  pthread_mutex_init(&relay_log_pos_mutex, NULL);
  queue = new query_queue*[opt_workers];
  url_for_binlog_api= new char[PATH_MAX+10];
  sql_thread_relay_log_path= new char[PATH_MAX+1];
  read_current_relay_info();
  pos= sql_thread_pos;
  print_log("Reading relay log file: %s from relay log pos: %lu",
            sql_thread_relay_log_path, sql_thread_pos);

  worker_thread_ids= new pthread_t[opt_workers];
  for (uint i=0; i< opt_workers; i++)
  {
    worker_info_t *info= new worker_info_t;
    memset(info, 0, sizeof(worker_info_t));
    queue[i]= new query_queue();
    info->worker_id= i;
    if (pthread_create(&(info->ptid), NULL, prefetch_worker, info))
    {
      print_log("ERROR: Failed to create worker worker_thread_ids!");
      goto err;
    }
    worker_thread_ids[i]= info->ptid;
  }
  if (pthread_create(&rli_reader_thread_id, NULL, rli_reader_thread, mysql))
  {
      print_log("ERROR: Failed to create relay log reader thread!");
      goto err;
  }
  dir_name_status_file = dirname(strdupa(opt_status_file));
  if (pthread_create(&status_thread_id, NULL, status_thread, NULL))
  {
    print_log("ERROR: Failed to create status thread!");
    goto err;
  }
  init_binlog_driver(sql_thread_relay_log_path, &url_for_binlog_api);
  DBUG_PRINT("Reading url %s pos: %lu", url_for_binlog_api, pos);

  gettimeofday(&t_begin, 0);
  print_log("Replication Booster started.");
  while (1)
  {
    status *status= read_binlog(binlog, pos, init);
    init= false;
    if (shutdown_program)
    {
      if (status)
        delete status;
      do_shutdown();
      goto end;
    }
    for (uint i=0; i < opt_workers; i++)
    {
      queue[i]->clear();
    }
    while (!is_sql_thread_running)
    {
      if (shutdown_program) {
        do_shutdown();
        goto end;
      }
      usleep(100000);
      continue;
    }
    read_current_relay_info();
    pos= sql_thread_pos;
    delete_binlog_driver();
    init_binlog_driver(sql_thread_relay_log_path, &url_for_binlog_api);
    delete status;
  }

end:
  exit(0);
err:
  exit(1);
}

