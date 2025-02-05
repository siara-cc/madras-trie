C = emcc
CXX = em++
CXXFLAGS = -std=c++11
INCLUDES = -I../src
L_FLAGS = -lnodefs.js
M_FLAGS =

opt: CXXFLAGS += -g -O2 -DNDEBUG
opt: madras_lookup

debug: CXXFLAGS += -g -O0 -fno-inline
debug: madras_lookup

release: CXXFLAGS += -O2 -funroll-loops -DNDEBUG
release: madras_lookup

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: madras_lookup

clean:
	rm madras_lookup
	rm -rf madras_lookup.dSYM

madras_lookup: madras_lookup.cpp ../src/common_dv1.hpp ../src/madras_dv1.hpp ../../ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) madras_lookup.cpp -o madras_lookup.js $(L_FLAGS) $(M_FLAGS) -sALLOW_MEMORY_GROWTH
