all: setup

setup: 
	clang -g -pthread -o node node.cpp monitor.cpp adaptor.cpp state_manager.cpp transfer_manager.cpp
clean:
	rm *.o node
