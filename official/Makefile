all: storage

.PHONY: storage clean

storage:
	~/15440/P2/go/bin/linux_amd64/storageserver -master=localhost:9999 &
	~/15440/P2/go/bin/linux_amd64/storageserver -master=localhost:9999 &
	~/15440/P2/go/bin/linux_amd64/storageserver -master=localhost:9999 &
	~/15440/P2/go/bin/linux_amd64/storageserver -port=9999 -N=4 &

clean:
	ps aux | grep storageserver | awk '!/grep/ { print $$2 | "xargs kill -9" }'
