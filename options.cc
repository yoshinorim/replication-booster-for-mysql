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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include "options.h"

uint opt_workers= 10;
uint opt_skip_events= 500;
uint opt_read_ahead_seconds= 3;
uint opt_sleep_millis_at_read_limit= 10000; // microseconds
char *opt_slave_user= "root";
char *opt_slave_password= "";
char *opt_admin_user= NULL;
char *opt_admin_password= NULL;
char *opt_slave_host= "localhost";
int opt_slave_port= 3306;
char *opt_slave_socket= NULL;
const char default_status_file[]= "/var/spool/replication_booster.log";
const char *opt_status_file= default_status_file;
uint opt_status_update_freq= 30;

struct option long_options[] =
{
  {"help", no_argument, 0, '?'},
  {"version", no_argument, 0, 'v'},
  {"threads", required_argument, 0, 't'},
  {"offset-events", required_argument, 0, 'o'},
  {"seconds-prefetch", required_argument, 0, 's'},
  {"mlllis-sleep", required_argument, 0, 'm'},
  {"user", required_argument, 0, 'u'},
  {"password", required_argument, 0, 'p'},
  {"admin_user", required_argument, 0, 'a'},
  {"admin_password", required_argument, 0, 'b'},
  {"host", required_argument, 0, 'h'},
  {"port", required_argument, 0, 'P'},
  {"socket", required_argument, 0, 'S'},
  {"status", required_argument, 0, 'f'},
  {"status-freq", required_argument, 0, 'F'},
  {0,0,0,0}
};

void usage()
{
  printf("Usage: \n");
  printf(" replication_booster [OPTIONS]\n\n");
  printf("Example: \n");
  printf(" replication_booster --user=mysql_select_user --password=mysql_select_pass --admin_user=mysql_root_user --admin_password=mysql_root_password --socket=/tmp/mysql.sock \n");
  printf("\n");
  printf("Options (short name):\n");
  printf(" -t, --threads=N                :Number of worker threads. Each worker thread converts binlog events and executes SELECT statements. Default is 10 (threads).\n");
  printf(" -o, --offset-events=N          :Number of binlog events that main thread (relay log reader thread) skips initially when reading relay logs. This number should be high when you have faster storage devices such as SSD. Default is 500 (events).\n");
  printf(" -s, --seconds-prefetch=N       :Main thread stops reading relay log events when the event's timestamp is --seconds-prefetch seconds ahead of current SQL thread's timestamp. After that the main thread starts reading relay logs from SQL threads's position again. If this value is too high, worker threads will execute many more SELECT statements than necessary. Default value is 3 (seconds).\n");
  printf(" -m, --millis-sleep=N           :If --seconds-prefetch condition is met, main thread sleeps --millis-sleep milliseconds before starting reading relay log. Default is 10 milliseconds.\n");
  printf(" -u, --user=mysql_user          :MySQL slave user name. This user should have at least SELECT privilege on all application tables (default: root)\n");
  printf(" -p, --password=mysql_pwd       :MySQL slave password (default: empty)\n");
  printf(" -a, --admin_user=mysql_user    :MySQL administration user for the slave. This user should have at least SUPER and REPLICATION CLIENT (for SHOW SLAVE STATUS) privileges. (default: root)\n");
  printf(" -b, --admin_password=mysql_pwd :MySQL password for the administration user. (default:empty)\n");
  printf(" -h, --host=mysql_host          :MySQL slave hostname or IP address. This must be local address. (default: localhost)\n");
  printf(" -P, --port=mysql_port          :MySQL slave port number (default:3306)\n");
  printf(" -S, --socket=mysql_socket      :MySQL socket file path\n");
  printf(" -f, --status=file              :Where to store the current status\n");
  printf(" -F, --status-freq=sec          :How often (in seconds) the status file is updated\n");
  printf("                                 Default is 30 seconds, 0 to disable.\n");
  exit(1);
}

void print_version()
{
  printf("replication_booster version %s\n", VER);
  exit(0);
}

void get_options(int argc, char **argv)
{
  int c, value, opt_ind= 0;
  while((c= getopt_long(argc, argv, "?vt:o:s:m:u:p:a:b:h:P:S:f:F:", long_options, &opt_ind)) != EOF)
  {
    switch(c)
    {
      case '?': usage(); break;
      case 'v': print_version(); break;
      case 't': value= atoi(optarg);
        opt_workers= value < 1 ? 1 : value;
        break;
      case 'o': value= atoi(optarg);
        opt_skip_events= value < 0 ? 0 : value;
        break;
      case 's': value= atoi(optarg);
        opt_read_ahead_seconds= value < 1 ? 1 : value;
        break;
      case 'm': opt_sleep_millis_at_read_limit= atoi(optarg)*1000; break;
      case 'u': opt_slave_user= optarg;  break;
      case 'p': opt_slave_password= optarg;  break;
      case 'a': opt_admin_user= optarg;  break;
      case 'b': opt_admin_password= optarg;  break;
      case 'h': opt_slave_host= optarg;  break;
      case 'P': opt_slave_port= atoi(optarg);  break;
      case 'S': opt_slave_socket= optarg;  break;
      case 'f': opt_status_file= optarg; break;
      case 'F': value= atoi(optarg);
        opt_status_update_freq= value < 1 ? 0 : value;
        break;
      default: usage();  break;
    }
  }
}

