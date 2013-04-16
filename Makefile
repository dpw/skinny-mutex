CFLAGS=-Wall -g -O6

.PHONY: all
all:: test perf-pthreads perf-skinny perf-spinlock

test: test.c skinny_mutex.c skinny_mutex.h
	$(CC) $(CFLAGS) -ansi -pthread skinny_mutex.c test.c -o $@ -lrt

define perf_target
perf-$(1): perf.c skinny_mutex.c skinny_mutex.h
	$$(CC) $$(CFLAGS) -DPERF_$(1) -ansi -pthread skinny_mutex.c perf.c -o $$@ -lrt
endef

$(eval $(call perf_target,pthreads))
$(eval $(call perf_target,skinny))
$(eval $(call perf_target,spinlock))

.PHONY: clean
clean::
	rm -rf test perf-pthreads perf-skinny perf-spinlock *~

.PHONY: coverage
coverage:
	$(MAKE) clean
	$(MAKE) test CFLAGS="$(CFLAGS) --coverage"
