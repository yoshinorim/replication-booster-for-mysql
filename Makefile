TARGET= replication_booster

SRCS= replication_booster.cc prefetch_worker.cc options.cc check_local.cc
OBJS= replication_booster.o prefetch_worker.o options.o check_local.o

DEST= /usr/local/bin
CXX= g++
CXXFLAGS= -c -Wall -O3 
LFLAGS= -Wall -O3 
LIBS= -lmysqlclient_r -lboost_regex -lreplication
DEBUG= -g -pg

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(LFLAGS) $(OBJS) $(LIBS) -o $(TARGET)

$(TARGET).o: $(SRCS)
	$(CXX) $(CXXFLAGS) -I/usr/include/mysql $(SRCS)

clean:
	rm -f *.o $(TARGET)

install: $(TARGET)
	install -s $(TARGET) $(DEST)

