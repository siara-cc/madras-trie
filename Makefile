C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native
INCLUDES = -I./src -I../ds_common/src -I../flavic48/src
L_FLAGS = 
M_FLAGS =
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: run_tests

debug: CXXFLAGS += -g -O0 -fno-inline
debug: run_tests

release: CXXFLAGS += -O2 -funroll-loops -DNDEBUG
release: run_tests

asan: CXXFLAGS += -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: run_tests

clean:
	rm run_tests
	rm -rf run_tests.dSYM

run_tests: run_tests_dv1.cpp src/common_dv1.hpp src/madras_dv1.hpp src/madras_builder_dv1.hpp ../leopard-trie/src/leopard.hpp ../ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -std=c++17 run_tests_dv1.cpp -o run_tests $(L_FLAGS) $(M_FLAGS)
