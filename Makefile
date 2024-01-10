C = gcc
CXXFLAGS = -pthread -march=native
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native
INCLUDES = -I./src
L_FLAGS = -lsnappy -llz4 -lbrotlienc -lbrotlidec -lz
M_FLAGS = -mbmi2 -mpopcnt
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O3 -funroll-loops -DNDEBUG
opt: run_tests

debug: CXXFLAGS += -g -O0 -fno-inline
debug: run_tests

release: CXXFLAGS += -O3 -fno-inline
release: run_tests

clean:
	rm run_tests
	rm -rf run_tests.dSYM

run_tests: run_tests_dv1.cpp src/madras_dv1.h src/madras_builder_dv1.h
	$(CXX) $(CXXFLAGS) $(INCLUDES) -std=c++11 run_tests_dv1.cpp -o run_tests
