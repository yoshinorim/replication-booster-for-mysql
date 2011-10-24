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
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h> 
#include <string.h> 
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include "replication_booster.h"

int check_local(const char *hostname_or_ip)
{
  struct ifaddrs *idaddr=NULL;
  struct hostent* hp;
  struct sockaddr_in addr;
  bool is_local= false;
  char ip_address[INET_ADDRSTRLEN];

  if ((hp = gethostbyname(hostname_or_ip)) == 0)
  {
    print_log("Error: %d -- gethostbyname(%s)", h_errno, hostname_or_ip);
    return 1;
  }
  if (hp->h_addrtype == AF_INET)
  {
    memset((char*)&addr, 0, sizeof(addr));
    memcpy((char*)&addr.sin_addr, hp->h_addr_list[0], hp->h_length);
    strcpy(ip_address, inet_ntoa(addr.sin_addr));
    DBUG_PRINT("Address: %s", ip_address);
  }
  getifaddrs(&idaddr);

  for (struct ifaddrs *ifa = idaddr; ifa != NULL; ifa = ifa->ifa_next)
  {
    if (ifa->ifa_addr->sa_family==AF_INET)
    { 
      void *tmp=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char buf[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmp, buf, INET_ADDRSTRLEN);
      DBUG_PRINT("%s IP Address %s", ifa->ifa_name, buf); 
      if (!strcmp(buf, ip_address))
        is_local= true;
    }
  }
  if (idaddr != NULL)
    freeifaddrs(idaddr);

  if (is_local)
  {
    return 0;
  } else {
    print_log("ERROR: Target hostname %s is not local address! Set local address.", hostname_or_ip);
     return 1;
  }
}

