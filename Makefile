CC = g++
VTCC = vtc++
#using C++11 standard.
CFLAGS = -std=c++11 -Wall
CDEBUG = -g

LIBS = -lssl -lcrypto

all : comparefs_omp comparefs_pth
all-debug : omp_debug pth_debug
all-profile : omp_vt pth_vt

clean :
	rm -vf comparefs_omp comparefs_pth

comparefs_omp : comparefs_omp.cpp
	$(CC) $(CFLAGS) -o comparefs_omp comparefs_omp.cpp -fopenmp $(LIBS)

comparefs_pth : comparefs_pthreads.cpp
	$(CC) $(CFLAGS) -o comparefs_pth comparefs_pthreads.cpp -lpthread $(LIBS)

omp_debug : comparefs_omp.cpp
	$(CC) $(CFLAGS) $(CDEBUG) -o comparefs_omp comparefs_omp.cpp -fopenmp $(LIBS)

pth_debug : comparefs_pthreads.cpp
	$(CC) $(CFLAGS) $(CDEBUG) -o comparefs_pth comparefs_pthreads.cpp -lpthread $(LIBS)

omp_vt : comparefs_omp.cpp
	export VT_MODE=STAT
	$(VTCC) -openmp $(CFLAGS) -o comparefs_omp comparefs_omp.cpp -fopenmp $(LIBS)

pth_vt : comparefs_pthreads.cpp
	export VT_MODE=STAT
	$(VTCC) -pthread $(CFLAGS) -o comparefs_pth comparefs_pthreads.cpp -lpthread $(LIBS)
