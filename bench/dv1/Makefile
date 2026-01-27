C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++11 -march=native -mpopcnt -mbmi -mbmi2 -mlzcnt -msse4 -msse3 -msse2
INCLUDES = -I../../include
L_FLAGS =
M_FLAGS = 
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O3 -DNDEBUG
opt: madras_bench

debug: CXXFLAGS += -g -O0 -fno-inline
debug: madras_bench

release: CXXFLAGS += -O3 -funroll-loops -DNDEBUG
release: madras_bench

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: madras_bench

clean:
	rm madras_bench
	rm -rf madras_bench.dSYM

madras_bench: madras_bench.cpp ../../include/madras/dv1/*.hpp ../../include/madras/dv1/builder/*.hpp ../../include/madras/dv1/builder/trie/*.hpp ../../include/madras/dv1/builder/trie/memtrie/*.hpp ../../include/madras/dv1/reader/*.hpp ../../include/madras/dv1/ds_common/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) madras_bench.cpp -o madras_bench $(L_FLAGS) $(M_FLAGS)
