#define _GNU_SOURCE

#include <time.h>
#include <assert.h>

#include "skinny_mutex.h"

static void test_simple(void)
{
	skinny_mutex_t mutex;
	static skinny_mutex_t static_mutex = SKINNY_MUTEX_INITIALIZER;

	assert(!skinny_mutex_init(&mutex));
	assert(!skinny_mutex_lock(&mutex));
	assert(!skinny_mutex_unlock(&mutex));
	assert(!skinny_mutex_destroy(&mutex));

	assert(!skinny_mutex_lock(&static_mutex));
	assert(!skinny_mutex_unlock(&static_mutex));
	assert(!skinny_mutex_destroy(&static_mutex));
}

struct test_contention {
	skinny_mutex_t mutex;
	int held;
	int count;
};

/* Wait a millisecond */
static void delay(void)
{
	struct timespec ts;
	ts.tv_sec = 0;
	ts.tv_nsec = 1000000;
	assert(!nanosleep(&ts, NULL));
}

static void *bump(void *v_tc)
{
	struct test_contention *tc = v_tc;

	assert(!skinny_mutex_lock(&tc->mutex));
	assert(!tc->held);
	tc->held = 1;
	delay();
	tc->held = 0;
	tc->count++;
	assert(!skinny_mutex_unlock(&tc->mutex));

	return NULL;
}

static void test_contention(void)
{
	struct test_contention tc;
	pthread_t threads[10];
	int i;

	assert(!skinny_mutex_init(&tc.mutex));
	assert(!skinny_mutex_lock(&tc.mutex));
	tc.held = 0;
	tc.count = 0;

	for (i = 0; i < 10; i++)
		assert(!pthread_create(&threads[i], NULL, bump, &tc));

	assert(!skinny_mutex_unlock(&tc.mutex));

	for (i = 0; i < 10; i++)
		assert(!pthread_join(threads[i], NULL));

	assert(!skinny_mutex_lock(&tc.mutex));
	assert(!tc.held);
	assert(tc.count == 10);
	assert(!skinny_mutex_unlock(&tc.mutex));
	assert(!skinny_mutex_destroy(&tc.mutex));
}

static void *lock_cancellation_thread(void *v_mutex)
{
	skinny_mutex_t *mutex = v_mutex;
	assert(!skinny_mutex_lock(mutex));
	assert(!skinny_mutex_unlock(mutex));
	return NULL;
}

/* skinny_mutex_lock is *not* a cancellation point. */
static void test_lock_cancellation(void)
{
	skinny_mutex_t mutex;
	pthread_t thread;
	void *retval;

	assert(!skinny_mutex_init(&mutex));
	assert(!skinny_mutex_lock(&mutex));
	assert(!pthread_create(&thread, NULL, lock_cancellation_thread,
			       &mutex));
	delay();
	assert(!pthread_cancel(thread));
	assert(!skinny_mutex_unlock(&mutex));
	assert(!pthread_join(thread, &retval));
	assert(!retval);
	assert(!skinny_mutex_destroy(&mutex));
}

static void *trylock_thread(void *v_mutex)
{
	skinny_mutex_t *mutex = v_mutex;
	assert(skinny_mutex_trylock(mutex) == EBUSY);
	return NULL;
}

static void *trylock_contender_thread(void *v_mutex)
{
	skinny_mutex_t *mutex = v_mutex;
	assert(!skinny_mutex_lock(mutex));
	delay();
	delay();
	assert(!skinny_mutex_unlock(mutex));
	return NULL;
}

static void test_trylock(void)
{
	skinny_mutex_t mutex;
	pthread_t thread1, thread2;

	assert(!skinny_mutex_init(&mutex));
	assert(!skinny_mutex_trylock(&mutex));

	assert(!pthread_create(&thread1, NULL, trylock_thread, &mutex));
	assert(!pthread_join(thread1, NULL));

	assert(!pthread_create(&thread1, NULL, trylock_contender_thread,
			       &mutex));
	delay();
	assert(!pthread_create(&thread2, NULL, trylock_thread, &mutex));
	assert(!pthread_join(thread2, NULL));
	assert(!skinny_mutex_unlock(&mutex));
	assert(!pthread_join(thread1, NULL));

	assert(!skinny_mutex_destroy(&mutex));
}

struct test_cond_wait {
	skinny_mutex_t mutex;
	pthread_cond_t cond;
	int flag;
};

static void *test_cond_wait_thread(void *v_tcw)
{
	struct test_cond_wait *tcw = v_tcw;

	assert(!skinny_mutex_lock(&tcw->mutex));
	while (!tcw->flag)
		assert(!skinny_mutex_cond_wait(&tcw->cond, &tcw->mutex));
	assert(!skinny_mutex_unlock(&tcw->mutex));

	return NULL;
}


static void test_cond_wait(void)
{
	struct test_cond_wait tcw;
	pthread_t thread;

	assert(!skinny_mutex_init(&tcw.mutex));
	assert(!pthread_cond_init(&tcw.cond, NULL));
	tcw.flag = 0;

	assert(!pthread_create(&thread, NULL, test_cond_wait_thread, &tcw));

	delay();
	assert(!skinny_mutex_lock(&tcw.mutex));
	tcw.flag = 1;
	assert(!pthread_cond_signal(&tcw.cond));
	assert(!skinny_mutex_unlock(&tcw.mutex));

	assert(!pthread_join(thread, NULL));

	assert(!skinny_mutex_destroy(&tcw.mutex));
	assert(!pthread_cond_destroy(&tcw.cond));
}

static void test_cond_timedwait(void)
{
	skinny_mutex_t mutex;
	pthread_cond_t cond;
	struct timespec t;

	assert(!skinny_mutex_init(&mutex));
	assert(!pthread_cond_init(&cond, NULL));

	assert(!clock_gettime(CLOCK_REALTIME, &t));

	t.tv_nsec += 1000000;
	if (t.tv_nsec > 1000000000) {
		t.tv_nsec -= 1000000000;
		t.tv_sec++;
	}

	assert(!skinny_mutex_lock(&mutex));
	assert(skinny_mutex_cond_timedwait(&cond, &mutex, &t) == ETIMEDOUT);
	assert(!skinny_mutex_unlock(&mutex));

	assert(!skinny_mutex_destroy(&mutex));
	assert(!pthread_cond_destroy(&cond));
}

static void test_cond_wait_cancellation(void)
{
	struct test_cond_wait tcw;
	pthread_t thread;
	void *retval;

	assert(!skinny_mutex_init(&tcw.mutex));
	assert(!pthread_cond_init(&tcw.cond, NULL));
	tcw.flag = 0;

	assert(!pthread_create(&thread, NULL, test_cond_wait_thread, &tcw));

	delay();
	assert(!pthread_cancel(thread));
	assert(!pthread_join(thread, &retval));
	assert(retval == PTHREAD_CANCELED);

	assert(!skinny_mutex_destroy(&tcw.mutex));
	assert(!pthread_cond_destroy(&tcw.cond));
}

int main(void)
{
	test_simple();
	test_contention();
	test_lock_cancellation();
	test_trylock();
	test_cond_wait();
	test_cond_timedwait();
	test_cond_wait_cancellation();
	return 0;
}
