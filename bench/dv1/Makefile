C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++17 -march=native
INCLUDES = -I../../include
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

madras_bench: madras_bench.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) madras_bench.cpp -o madras_bench $(L_FLAGS) $(M_FLAGS)
