all: setup

setup: 
	gcc -g -pthread -o node node.c monitor.c adaptor.c state_manager.c transfer_manager.c
clean:
	rm *.o node
