#define _GNU_SOURCE

#include <stdlib.h>
#include <time.h>
#include <assert.h>

#include "skinny_mutex.h"

static void test_static_mutex(void)
{
	static skinny_mutex_t static_mutex = SKINNY_MUTEX_INITIALIZER;

	assert(!skinny_mutex_lock(&static_mutex));
	assert(!skinny_mutex_unlock(&static_mutex));
	assert(!skinny_mutex_destroy(&static_mutex));
}

static void test_lock_unlock(skinny_mutex_t *mutex)
{
	assert(!skinny_mutex_lock(mutex));
	assert(!skinny_mutex_unlock(mutex));
}

/* Wait a millisecond */
static void delay(void)
{
	struct timespec ts;
	ts.tv_sec = 0;
	ts.tv_nsec = 1000000;
	assert(!nanosleep(&ts, NULL));
}

struct test_contention {
	skinny_mutex_t *mutex;
	int held;
	int count;
};

static void *bump(void *v_tc)
{
	struct test_contention *tc = v_tc;

	assert(!skinny_mutex_lock(tc->mutex));
	assert(!tc->held);
	tc->held = 1;
	delay();
	tc->held = 0;
	tc->count++;
	assert(!skinny_mutex_unlock(tc->mutex));

	return NULL;
}

static void test_contention(skinny_mutex_t *mutex)
{
	struct test_contention tc;
	pthread_t threads[10];
	int i;

	tc.mutex = mutex;
	tc.held = 0;
	tc.count = 0;

	assert(!skinny_mutex_lock(tc.mutex));

	for (i = 0; i < 10; i++)
		assert(!pthread_create(&threads[i], NULL, bump, &tc));

	assert(!skinny_mutex_unlock(tc.mutex));

	for (i = 0; i < 10; i++)
		assert(!pthread_join(threads[i], NULL));

	assert(!skinny_mutex_lock(tc.mutex));
	assert(!tc.held);
	assert(tc.count == 10);
	assert(!skinny_mutex_unlock(tc.mutex));
}

static void *lock_cancellation_thread(void *v_mutex)
{
	skinny_mutex_t *mutex = v_mutex;
 	assert(!skinny_mutex_lock(mutex));
	assert(!skinny_mutex_unlock(mutex));
	return NULL;
}

/* skinny_mutex_lock is *not* a cancellation point. */
static void test_lock_cancellation(skinny_mutex_t *mutex)
{
	pthread_t thread;
	void *retval;

	assert(!skinny_mutex_lock(mutex));
	assert(!pthread_create(&thread, NULL, lock_cancellation_thread,
			       mutex));
	delay();
	assert(!pthread_cancel(thread));
	assert(!skinny_mutex_unlock(mutex));
	assert(!pthread_join(thread, &retval));
	assert(!retval);
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

static void test_trylock(skinny_mutex_t *mutex)
{
	pthread_t thread1, thread2;

	assert(!skinny_mutex_trylock(mutex));

	assert(!pthread_create(&thread1, NULL, trylock_thread, mutex));
	assert(!pthread_join(thread1, NULL));

	assert(!pthread_create(&thread1, NULL, trylock_contender_thread,
			       mutex));
	delay();
	assert(!pthread_create(&thread2, NULL, trylock_thread, mutex));
	assert(!pthread_join(thread2, NULL));
	assert(!skinny_mutex_unlock(mutex));
	assert(!pthread_join(thread1, NULL));
}

struct test_cond_wait {
	skinny_mutex_t *mutex;
	pthread_cond_t cond;
	int flag;
};

static void *test_cond_wait_thread(void *v_tcw)
{
	struct test_cond_wait *tcw = v_tcw;

	assert(!skinny_mutex_lock(tcw->mutex));
	while (!tcw->flag)
		assert(!skinny_mutex_cond_wait(&tcw->cond, tcw->mutex));
	assert(!skinny_mutex_unlock(tcw->mutex));

	return NULL;
}

static void test_cond_wait(skinny_mutex_t *mutex)
{
	struct test_cond_wait tcw;
	pthread_t thread;

	tcw.mutex = mutex;
	assert(!pthread_cond_init(&tcw.cond, NULL));
	tcw.flag = 0;

	assert(!pthread_create(&thread, NULL, test_cond_wait_thread, &tcw));

	delay();
	assert(!skinny_mutex_lock(mutex));
	tcw.flag = 1;
	assert(!pthread_cond_signal(&tcw.cond));
	assert(!skinny_mutex_unlock(mutex));

	assert(!pthread_join(thread, NULL));

	assert(!pthread_cond_destroy(&tcw.cond));
}

static void test_cond_timedwait(skinny_mutex_t *mutex)
{
	pthread_cond_t cond;
	struct timespec t;

	assert(!pthread_cond_init(&cond, NULL));

	assert(!clock_gettime(CLOCK_REALTIME, &t));

	t.tv_nsec += 1000000;
	if (t.tv_nsec > 1000000000) {
		t.tv_nsec -= 1000000000;
		t.tv_sec++;
	}

	assert(!skinny_mutex_lock(mutex));
	assert(skinny_mutex_cond_timedwait(&cond, mutex, &t) == ETIMEDOUT);
	assert(!skinny_mutex_unlock(mutex));

	assert(!pthread_cond_destroy(&cond));
}

static void test_cond_wait_cancellation(skinny_mutex_t *mutex)
{
	struct test_cond_wait tcw;
	pthread_t thread;
	void *retval;

	tcw.mutex = mutex;
	assert(!pthread_cond_init(&tcw.cond, NULL));
	tcw.flag = 0;

	assert(!pthread_create(&thread, NULL, test_cond_wait_thread, &tcw));

	delay();
	assert(!pthread_cancel(thread));
	assert(!pthread_join(thread, &retval));
	assert(retval == PTHREAD_CANCELED);

	assert(!pthread_cond_destroy(&tcw.cond));
}

static void test_unlock_not_held(skinny_mutex_t *mutex)
{
	assert(skinny_mutex_unlock(mutex) == EPERM);
}

static void *test_transfer_thread(void *v_mutexes)
{
	skinny_mutex_t *mutexes = v_mutexes;
	int res;

	assert(!skinny_mutex_lock(&mutexes[0]));
	res = skinny_mutex_transfer(&mutexes[0], &mutexes[1]);

	if (res == EAGAIN) {
		assert(!skinny_mutex_unlock(&mutexes[0]));
	}
	else {
		assert(!res);
		assert(!skinny_mutex_unlock(&mutexes[1]));
	}

	return (void *)(size_t)res;
}

static void test_transfer(skinny_mutex_t *mutexes)
{
	pthread_t thread;
	void *retval;

	/* Check EPERM result */
	assert(skinny_mutex_transfer(&mutexes[0], &mutexes[1]) == EPERM);

	/* Test the case where the transfer doesn't need to wait */
	assert(!skinny_mutex_lock(&mutexes[0]));
	assert(!skinny_mutex_transfer(&mutexes[0], &mutexes[1]));
	assert(!skinny_mutex_unlock(&mutexes[1]));

	/* Check that A was unlocked. */
	assert(!skinny_mutex_trylock(&mutexes[0]));
	assert(!skinny_mutex_unlock(&mutexes[0]));

	/* Make a transfer wait, but then allow it to complete. */
	assert(!skinny_mutex_lock(&mutexes[1]));
	assert(!pthread_create(&thread, NULL, test_transfer_thread, mutexes));
	delay();
	assert(!skinny_mutex_unlock(&mutexes[1]));
	assert(!pthread_join(thread, &retval));
	assert(retval == (void *)0);

	/* Make a transfer wait, and then veto it. */
	assert(!skinny_mutex_lock(&mutexes[1]));
	assert(!pthread_create(&thread, NULL, test_transfer_thread, mutexes));
	delay();
	assert(!skinny_mutex_veto_transfer(&mutexes[1]));
	assert(!pthread_join(thread, &retval));
	assert(retval == (void *)EAGAIN);
	assert(!skinny_mutex_unlock(&mutexes[1]));
}

static void test_transfer_veto(skinny_mutex_t *mutex)
{
	/* Veto on a mutex with no transfers occuring */
	assert(!skinny_mutex_lock(mutex));
	assert(!skinny_mutex_veto_transfer(mutex));
	assert(!skinny_mutex_unlock(mutex));

	/* Veto on an unheld mutex gives EPERM. */
	assert(skinny_mutex_veto_transfer(mutex) == EPERM);
}

struct do_test {
	skinny_mutex_t *mutex;
	pthread_cond_t cond;
	int phase;
};

static void *do_test_cond_thread(void *v_dt)
{
	struct do_test *dt = v_dt;

	assert(!skinny_mutex_lock(dt->mutex));
	dt->phase = 1;
	assert(!pthread_cond_signal(&dt->cond));

	do {
		assert(!skinny_mutex_cond_wait(&dt->cond, dt->mutex));
	} while (dt->phase != 2);

	assert(!skinny_mutex_unlock(dt->mutex));

	return NULL;
}

static void do_test_aux(int i, skinny_mutex_t *mutexes,
			void (*f)(skinny_mutex_t *m))
{
	struct do_test dt;
	pthread_t thread;

	if (i == 0) {
		f(mutexes);
		return;
	}

	i--;

	/* First do the test with a fresh mutex. */
	assert(!skinny_mutex_init(mutexes + i));
	do_test_aux(i, mutexes, f);
	assert(!skinny_mutex_destroy(mutexes + i));

	/* Do the test with a thread waiting on a cond var associated
	   with the mutex.  This ensures that the skinny mutex has a
	   fat mutex during the text. */
	assert(!skinny_mutex_init(mutexes + i));

	dt.mutex = mutexes + i;
	assert(!pthread_cond_init(&dt.cond, NULL));
	dt.phase = 0;

	assert(!pthread_create(&thread, NULL, do_test_cond_thread, &dt));

	/* Wait until the thread is waiting on the cond var */
	assert(!skinny_mutex_lock(mutexes + i));
	while (dt.phase != 1) {
		assert(!skinny_mutex_cond_wait(&dt.cond, mutexes + i));
	};
	assert(!skinny_mutex_unlock(mutexes + i));

	do_test_aux(i, mutexes, f);

	assert(!skinny_mutex_lock(mutexes + i));
	dt.phase = 2;
	assert(!pthread_cond_signal(&dt.cond));
	assert(!skinny_mutex_unlock(mutexes + i));

	assert(!pthread_join(thread, NULL));
	assert(!skinny_mutex_destroy(mutexes + i));
	assert(!pthread_cond_destroy(&dt.cond));
}

static void do_test(void (*f)(skinny_mutex_t *m))
{
	skinny_mutex_t mutex;
	do_test_aux(1, &mutex, f);
}

static void do_test_multi(void (*f)(skinny_mutex_t *m), int n)
{
	skinny_mutex_t *mutexes = malloc(n * sizeof *mutexes);
	assert(mutexes);
	do_test_aux(n, mutexes, f);
	free(mutexes);
}

int main(void)
{
	test_static_mutex();

	do_test(test_lock_unlock);
	do_test(test_contention);
	do_test(test_lock_cancellation);
	do_test(test_trylock);
	do_test(test_cond_wait);
	do_test(test_cond_timedwait);
	do_test(test_cond_wait_cancellation);
	do_test(test_unlock_not_held);
	do_test_multi(test_transfer, 2);
	do_test(test_transfer_veto);

	return 0;
}
