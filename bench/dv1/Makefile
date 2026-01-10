C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++17 -march=native
INCLUDES = -I../../src -I../../src/dv1 -I../../src/dv1/reader -I../../src/dv1/builder -I../../src/ds_common/src -I../../src/allflic48/src
L_FLAGS =
M_FLAGS = 
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: madras_bench

debug: CXXFLAGS += -g -O0 -fno-inline
debug: madras_bench

release: CXXFLAGS += -O2 -funroll-loops -DNDEBUG
release: madras_bench

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: madras_bench

clean:
	rm madras_bench
	rm -rf madras_bench.dSYM

madras_bench: madras_bench.cpp ../../src/dv1/*.hpp ../../src/dv1/reader/*.hpp ../../src/dv1/builder/*.hpp ../../src/leopard-trie/src/leopard.hpp ../../src/ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) madras_bench.cpp -o madras_bench $(L_FLAGS) $(M_FLAGS)
