C = gcc
CXX = g++
CXXFLAGS = -pthread -std=c++17 -march=native
INCLUDES = -I./src -I../../ds_common/src -I../../flavic48/src
L_FLAGS = 
M_FLAGS = -mbmi2 -mpopcnt
#OBJS = build/imain.o

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: query_mdx

debug: CXXFLAGS += -g -O0 -fno-inline
debug: query_mdx

release: CXXFLAGS += -O3 -funroll-loops -DNDEBUG
release: query_mdx

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: query_mdx

clean:
	rm query_mdx
	rm -rf query_mdx.dSYM

query_mdx: query_mdx.cpp ../src/common_dv1.hpp ../src/madras_dv1.hpp ../src/madras_builder_dv1.hpp ../../leopard-trie/src/leopard.hpp ../../ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) query_mdx.cpp -o query_mdx $(L_FLAGS) $(M_FLAGS)
