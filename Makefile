CXX		= g++ -std=c++20
INCLUDES	= -I src/include -I src/fastflow
CXXFLAGS  	+= -Wall -Werror -pedantic

LDFLAGS 	= -pthread -fopenmp
OPTFLAGS	= -O3 -ffast-math -march=native

TARGETS		= mergesort_seq mergesort_ff

.PHONY: all clean cleanall
.SUFFIXES: .cpp

SRC_DIR=src

%: $(SRC_DIR)/%.cpp $(SRC_DIR)/include
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

all	: $(TARGETS)


clean: 
	rm -f $(TARGETS)

cleanall: clean
	rm -f *.o *~ *.csv
