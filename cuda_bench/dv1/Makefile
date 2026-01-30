C = nvcc
CXX = nvcc
CXXFLAGS = -std=c++11 -arch=sm_61
INCLUDES = -I../../include
L_FLAGS = 
M_FLAGS = 
#OBJS = build/imain.o

opt: CXXFLAGS += -O2 -DNDEBUG
opt: cuda_bench

debug: CXXFLAGS += -g -O0 -fno-inline
debug: cuda_bench

release: CXXFLAGS += -O2 -DNDEBUG
release: cuda_bench

asan: CXXFLAGS += -static-libsan -fno-inline -O0 -g -fsanitize=address -fno-omit-frame-pointer
asan: cuda_bench

clean:
	rm cuda_bench

cuda_bench: cuda_bench.cu
	$(CXX) $(CXXFLAGS) $(INCLUDES) cuda_bench.cu -o cuda_bench $(L_FLAGS) $(M_FLAGS)
