CXX        = g++ -std=c++20
INCLUDES   = -I src/include -I src/fastflow
CXXFLAGS  += -Wall -Werror -pedantic

LDFLAGS    = -pthread 
OPTFLAGS   = -O3 -ffast-math -march=native
DOPTFLAGS  = -g -O0 -lprofiler

TARGETS    = mergesort_seq mergesort_ff
DEBUG_TARGETS = $(addsuffix _debug, $(TARGETS))

SRC_DIR = src

.PHONY: all debug clean cleanall
.SUFFIXES: .cpp

# Default optimized build
%: $(SRC_DIR)/%.cpp $(SRC_DIR)/include
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

# Debug builds with _debug suffix
%_debug: $(SRC_DIR)/%.cpp $(SRC_DIR)/include
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(DOPTFLAGS) -o $@ $< $(LDFLAGS)

all: $(TARGETS)

debug: $(DEBUG_TARGETS)

clean:
	rm -f $(TARGETS) $(DEBUG_TARGETS)

cleanall: clean
	rm -f *.o *~ *.csv
