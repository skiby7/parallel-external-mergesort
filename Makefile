CXX        = g++ -std=c++20
INCLUDES   = -I src/include -I src/fastflow
CXXFLAGS  += -Wall -Werror -pedantic -Wno-unused-function

LDFLAGS    = -pthread -fopenmp
OPTFLAGS   = -O3 -ffast-math -march=native
DOPTFLAGS  = -g -O0
TARGETS    = mergesort_seq mergesort_omp mergesort_ff mergesort_mpi
DEBUG_TARGETS = $(addsuffix _debug, $(TARGETS))

SRC_DIR = src

.PHONY: all debug clean cleanall
.SUFFIXES: .cpp

all: $(TARGETS) gen_file

debug: $(DEBUG_TARGETS)

gen_file: $(SRC_DIR)/gen_file.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o gen_file $(SRC_DIR)/gen_file.cpp $(LDFLAGS)

test_seq_gen: $(SRC_DIR)/test_seq_gen.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o test_seq_gen $(SRC_DIR)/test_seq_gen.cpp $(LDFLAGS)

mergesort_seq: $(SRC_DIR)/mergesort_seq.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_omp: $(SRC_DIR)/mergesort_omp.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_ff: $(SRC_DIR)/mergesort_ff.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_mpi: $(SRC_DIR)/mergesort_mpi.cpp $(SRC_DIR)/include/
	mpicxx $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $<  $(LDFLAGS) -lmpi

mergesort_seq_debug: $(SRC_DIR)/mergesort_seq.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(DOPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_omp_debug: $(SRC_DIR)/mergesort_omp.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(DOPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_ff_debug: $(SRC_DIR)/mergesort_ff.cpp $(SRC_DIR)/include/
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(DOPTFLAGS) -o $@ $< $(LDFLAGS)

mergesort_mpi_debug: $(SRC_DIR)/mergesort_mpi.cpp $(SRC_DIR)/include/
	mpicxx $(CXXFLAGS) $(INCLUDES) $(DOPTFLAGS) -o $@ $< $(LDFLAGS) -lmpi



clean:
	rm -f $(TARGETS) $(DEBUG_TARGETS) gen_file

cleanall: clean
	rm -f *.o *~ *.csv
