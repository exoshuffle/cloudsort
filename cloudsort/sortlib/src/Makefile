CXX = g++
CXXFLAGS = -pthread -Wall -Wextra -O3 -march=native
# CXX = /opt/intel/oneapi/compiler/latest/linux/bin/icpx
# CXXFLAGS = -pthread -Wall -Wextra -Ofast -xHost

SRC = *.cpp
TARGET = test

run: $(TARGET)
	./$(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

.PHONY: clean
clean:
	rm $(TARGET)
