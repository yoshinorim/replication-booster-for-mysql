project(replication-booster)
cmake_minimum_required(VERSION 2.6)

set(SOURCE replication_booster.cc prefetch_worker.cc options.cc check_local.cc)

# Find MySQL client library and header files
find_library(MySQL_LIBRARY mysqlclient)
find_path(MySQL_HEADER mysql.h  /usr/local/include/mysql /usr/include/mysql)
include_directories(${MySQL_HEADER})

# Find MySQL replication listener and header files
find_library(Replication_LIBRARY replication)
find_path(Replication_HEADER binlog_api.h)
include_directories(${Replication_HEADER})

# Find Boost
set(Boost_DEBUG FALSE)
set(Boost_FIND_REQUIRED TRUE)
set(Boost_FIND_QUIETLY TRUE)
set(Boost_USE_STATIC_LIBS TRUE)
find_package(Boost REQUIRED system thread regex)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(replication_booster ${SOURCE})
target_link_libraries(replication_booster ${Boost_LIBRARIES} ${Replication_LIBRARY} ${MySQL_LIBRARY})

install(TARGETS replication_booster DESTINATION bin)
