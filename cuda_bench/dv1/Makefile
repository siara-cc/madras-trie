C = nvcc
CXX = nvcc
CXXFLAGS = -std=c++17 -arch=sm_61
INCLUDES = -I../src -I../../ds_common/src -I../../flavic48/src
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

cuda_bench: cuda_bench_dv1.cu ../src/*.hpp ../../ds_common/src/*.hpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -std=c++17 cuda_bench_dv1.cu -o cuda_bench $(L_FLAGS) $(M_FLAGS)
