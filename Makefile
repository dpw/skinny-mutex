CFLAGS=-Wall -g

test: skinny_mutex.c test.c skinny_mutex.h
	$(CC) $(CFLAGS) -ansi -pthread skinny_mutex.c test.c -o test -lrt

.PHONY: clean
clean::
	rm -rf test *~

.PHONY: coverage
coverage:
	$(MAKE) clean
	$(MAKE) test CFLAGS="$(CFLAGS) --coverage"
