C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native
INCLUDES = -I./include
L_FLAGS = 
M_FLAGS =
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: run_tests

debug: CXXFLAGS += -g -O0 -fno-inline
debug: run_tests

release: CXXFLAGS += -O2 -funroll-loops -DNDEBUG
release: run_tests

asan: CXXFLAGS += -fno-inline -O0 -g -fsanitize=undefined,address -fno-omit-frame-pointer
asan: run_tests

clean:
	rm run_tests
	rm -rf run_tests.dSYM

run_tests: run_tests_dv1.cpp include/madras/dv1/common.hpp include/madras/dv1/reader/*.hpp include/madras/dv1/builder/*.hpp ./include/madras/dv1/ds_common/*.hpp ./include/madras/dv1/allflic48/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -std=c++17 run_tests_dv1.cpp -o run_tests $(L_FLAGS) $(M_FLAGS)
