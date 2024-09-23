C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native
INCLUDES = -I./src
L_FLAGS = -lsnappy -llz4 -lbrotlienc -lbrotlidec -lz -lcds
M_FLAGS = -mbmi2 -mpopcnt
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: run_tests

debug: CXXFLAGS += -g -O0 -fno-inline
debug: run_tests

release: CXXFLAGS += -O2 -funroll-loops -DNDEBUG
release: run_tests

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: run_tests

clean:
	rm run_tests
	rm -rf run_tests.dSYM

run_tests: run_tests_dv1.cpp src/common_dv1.hpp src/madras_dv1.hpp src/madras_builder_dv1.hpp ../leopard-trie/src/leopard.hpp ../ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -std=c++11 run_tests_dv1.cpp -o run_tests $(L_FLAGS) $(M_FLAGS)
