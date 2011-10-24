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
#include <boost/regex.hpp>

uint64_t stat_popped_queries= 0;
uint64_t stat_old_queries= 0;
uint64_t stat_discarded_queries= 0;
uint64_t stat_converted_queries= 0;
uint64_t stat_executed_selects= 0;
uint64_t stat_error_selects= 0;

const char *update_pattern= "\\A.*?update(?:\\s+(?:low_priority|ignore))?\\s+(.*?)\\s+set\\b(.*?)(?:\\s*where\\b(.*?))?(limit\\s*[0-9]+(?:\\s*,\\s*[0-9]+)?)?\\Z";
const char *delete_pattern= "\\A.*?delete\\s(.*?)\\bfrom\\b(.*)\\Z";

const boost::regex update_exp(update_pattern,
  boost::regbase::normal | boost::regbase::icase);
const boost::regex delete_exp(delete_pattern,
  boost::regbase::normal | boost::regbase::icase);

static char* convert_to_select(const std::string query, uint *length)
{
  std::string select;
  char *buf;
  boost::smatch result;

  if (boost::regex_search(query, result, update_exp))
  {
    DBUG_PRINT("Match UPDATE.");
    select = "select isnull(coalesce(";
    select.append(result.str(2));
    select.append(")) from ");
    select.append(result.str(1));
    if (result.position(3) != -1)
    {
      select.append(" where ");
      select.append(result.str(3));
    }
    if (result.position(4) != -1)
    {
      select.append(" ");
      select.append(result.str(4));
    }
    DBUG_PRINT(result.str(1));
    DBUG_PRINT(result.str(2));
    DBUG_PRINT(result.str(3));
    DBUG_PRINT(result.str(4));
  } else if (boost::regex_search(query, result, delete_exp))
  {
    DBUG_PRINT("Match DELETE.");
    select = "select * from ";
    select.append(result.str(2));
    DBUG_PRINT(result.str(1));
    DBUG_PRINT(result.str(2));
  } else
  {
    DBUG_PRINT("Not matched UPDATE/DELETE.");
    goto unmatch;
  }

  DBUG_PRINT(select);
  *length= select.length();
  buf= new char[*length + 1];
  strcpy(buf, select.c_str());
  return buf;

unmatch:
  return NULL;
}


void* prefetch_worker(void *worker_info)
{
  int ret= 0;
  MYSQL *mysql;
  MYSQL_RES   *result;
  char current_db[1024]= "";
  worker_info_t *info= (worker_info_t*)worker_info;
  uint worker_id= info->worker_id;
  uint64_t popped_queries= 0;
  uint64_t old_queries= 0;
  uint64_t discarded_queries= 0;
  uint64_t converted_queries= 0;
  uint64_t executed_selects= 0;
  uint64_t error_selects= 0;

  mysql= mysql_init((MYSQL*)0);
  if (!mysql)
  {
    print_log("ERROR: mysql_init failed on worker.");
    goto err;
  }
  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "client");
  if ( !mysql_real_connect(mysql, opt_slave_host, opt_slave_user, opt_slave_password, NULL, opt_slave_port, opt_slave_socket, 0) )
  {
    print_log("ERROR: Worker failed to connect to MySQL: %d, %s", mysql_errno(mysql),mysql_error(mysql));
    goto err;
  }
  query_t *query;

  while (1)
  {
    query= queue[worker_id]->wait_and_pop();
    if (query->shutdown)
    {
      delete query;
      goto end;
    }
    popped_queries++;

    if (query->pos <= sql_thread_pos)
    {
      old_queries++;
      free_query(query);
      continue;
    }

    const mysql::Query_event *qev= query->qev;
    uint select_len;
    char* select_query= convert_to_select(qev->query, &select_len);
    if (select_query != NULL)
    {
      converted_queries++;
      // database has changed
      if (strcmp(current_db, qev->db_name.c_str()))
      {
        strcpy(current_db, qev->db_name.c_str());
        DBUG_PRINT("Database has changed. worker id=%d, %s", info->worker_id, current_db);
        if (mysql_select_db(mysql, current_db))
        {
          print_log("ERROR: Failed to change db: %s %d %s", current_db, mysql_errno(mysql),mysql_error(mysql));
          goto err;
        }
      }
      ret= mysql_real_query(mysql, select_query, select_len);
      if (ret)
      {
        print_log("ERROR: Got error on query. Error code:%d message:%s. query:%s", mysql_errno(mysql),mysql_error(mysql), select_query);
        error_selects++;
      } else
      {
        executed_selects++;
      }
      free_query(query, select_query);
      result = mysql_store_result(mysql);
      mysql_free_result(result);
    } else
    {
      free_query(query);
    }
    if (shutdown_program)
      goto end;
  }

end:
err:
  if (info)
    delete info;
  if (mysql)
    mysql_close(mysql);
  mysql_thread_end();
  pthread_mutex_lock(&worker_mutex);
  stat_popped_queries += popped_queries;
  stat_old_queries += old_queries;
  stat_discarded_queries += discarded_queries;
  stat_converted_queries += converted_queries;
  stat_executed_selects += executed_selects;
  stat_error_selects += error_selects;
  pthread_mutex_unlock(&worker_mutex);
  pthread_exit(0);
}

