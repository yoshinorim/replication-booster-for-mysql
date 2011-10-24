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

#ifndef __options_h_
#define __options_h_

#include "replication_booster.h"

extern uint opt_workers;
extern uint opt_skip_events;
extern uint opt_read_ahead_seconds;
extern uint opt_sleep_millis_at_read_limit;
extern char *opt_slave_user;
extern char *opt_slave_password;
extern char *opt_admin_user;
extern char *opt_admin_password;
extern char *opt_slave_host;
extern int opt_slave_port;
extern char *opt_slave_socket;

void get_options(int argc, char **argv);

#endif
