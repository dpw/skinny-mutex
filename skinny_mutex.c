#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <errno.h>

#include "skinny_mutex.h"

/* The alternative definition of cas can be used to induce random
 * failures in CAS operations.  This is only useful in situations
 * which can recover from false negatives.  So in the cases where a
 * failed CAS is significant, we use strict_cas. */

#if 1
#define cas(p, a, b) __sync_bool_compare_and_swap(p, a, b)
#else
#define cas(p, a, b) ((rand() & 1) ? __sync_bool_compare_and_swap(p, a, b) : 0)
#endif

#define strict_cas(p, a, b) __sync_bool_compare_and_swap(p, a, b)

/* Atomically exchange the value of a pointer in memory.
 *
 * This is absent from GCC's builtin atomics, but we can simulate it
 * with CAS.
 */
static void *atomic_xchg(void **ptr, void *new)
{
#if defined(__i386__)
	void *old = new;
	__asm__ volatile ("xchgl %0, %1\n" /* lock prefix is implicit */
		      : "+r" (old), "+m" (*ptr)
		      : : "memory", "cc");
	return old;
#elif defined(__x86_64__)
	void *old = new;
	__asm__ volatile ("xchgq %0, %1\n" /* lock prefix is implicit */
		      : "+r" (old), "+m" (*ptr)
		      : : "memory", "cc");
	return old;
#else
	void *old;
	do
		old = *ptr;
	while (!cas(ptr, old, new));
	return old;
#endif
}

/* Atomically subtract from a byte in memory, and test the subsequent
 * value, returning zero if it reached zero, and non-zero otherwise.
 *
 * This is similar to GCC's __sync_sub_and_fetch builtin, and we can
 * use that instead.  But on x86, GCC has to use CAS in a loop to
 * implement __sync_sub_and_fetch so that it can provide the previous
 * value.  Because this function only returns a zero/non-zero
 * indication, it can be implemented with a locked sub instruction
 * instead.
 */
static int atomic_sub_and_test(uint8_t *ptr, uint8_t x)
{
#if defined(__i386__) || defined(__x86_64__)
	uint8_t res;
        __asm__ volatile ("lock subb %2,%0; setne %1"
			  : "+m" (*ptr), "=qm" (res)
			  : "ir" (x) : "memory");
	return res;
#else
	return __sync_sub_and_fetch(ptr, x);
#endif
}


/* The common header for the fat_mutex and peg structs */
struct common {
	uint8_t peg;
};

/*
 * A skinny_mutex_t contains a pointer-sized word.  The non-contended
 * cases is simple: If the mutex is not held, it contains 0.  If the
 * mutex is held but not contended, it contains 1.  A compare-and-swap
 * is used to acquire an unheld skinny_mutex, or to release it when
 * held.
 *
 * When a lock becomes contended - when a thread tries to lock a
 * skinny_mutex that is alreay held - we fall back to standard
 * pthreads synchronization primitives (so that the thread can block
 * and be woken again when it has a chance to acquire the lock).  The
 * fat_mutex struct holds all the state necessary to handle contention
 * cases (that is, a normal pthreads mutex and condition variable, and
 * a flag to indicate whether the skinny_mutex is held or not).
 */

struct fat_mutex {
	struct common common;

	/* Is the lock held? */
	uint8_t held;

	/* How many threads are waiting to acquire the associated
	 * skinny_mutex. */
	unsigned long waiters;

	/* References that prevent the fat_mutex being freed.  This
	 * includes:
	 *
	 * - References from threads waiting to acquire the
	 * mutex.
	 *
	 * - References from pegs (see below) not on the primary chain
	 * (qnother way of looking at it is that we do include the
	 * reference from the primary chain, which could be the one
	 * from the skinny_mutex, but we offset the refcount value by
	 * -1, so a refcount of 0 means we only have the primary
	 * chain).
	 *
	 * - A pseudo-reference from the thread holding the skinny_mutex
         * (this might not correspond to an explicit reference, but
         * keeps the fat_mutex pinned while the mutex is held).
	 *
	 * - References from thread waiting on condition variables
	 * associated with the skinny_mutex.
	 */
	unsigned long refcount;

	/* The pthreads mutex guarding the other fields. */
	pthread_mutex_t mutex;

	/* Conv var signalled when the mutex is released and there are
	   waiters. */
	pthread_cond_t held_cond;
};

/*
 * If the skinny_mutex points to a fat_mutex, a thread cannot simply
 * obtain the pointer and dereference it, as another thread might free
 * the fat_mutex.  We need to track whether other threads are
 * intending to access the fat_mutex, to delay freeing it if so.
 *
 * Many lock-free algoithms solve, this problem using hazard pointers.
 * But hazard pointers require tracking the set of threads involved,
 * and deallocations must be batched and implemented with a data
 * structure to allow efficient comparison of a candidate pointer with
 * the set of hazard pointers.  All this requires a substantial amount
 * of code.
 *
 * We use a simpler approach: Pegging.  (This approach is slightly
 * more expensive that hazard pointers on a per-access basis, but we
 * only access when contention or other much higher costs are
 * involved.)

 * A thread indicates its intent to access the fat_mutex by allocating
 * a peg struct and storing a pointer to it into the skinny_mutex,
 * replacing the pointer to the fat_mutex (see fat_mutex_peg).  The
 * fat_mutex is only freed if the skinny_mutex points directly to it,
 * so the peg prevents it being freed (see fat_mutex_release).
 *
 * The peg struct has a "next" pointer in it, pointing to the previous
 * value of the skinny_mutex (skinny_mutex is updated with CAS so that
 * installing a peg is atomic).  So chains of pegs can be built up,
 * starting with the skinny_mutex, followed by zero or more pegs, and
 * terminating with the fat_mutex, e.g.:
 *
 * +--------------+   +--------+   +--------+   +-----------+
 * | skinny_mutex |   |   peg  |   |   peg  |   | fat_mutex |
 * +--------------+   +--------+   +--------|   +-----------+
 * | val  *---------->| next *---->| next *---->|   ...     |
 * +--------------+   |   ...  |   |   ...  |   +-----------+
 *                    +--------+   +--------+
 *
 * During the process of releasing a peg, the skinny_mutex is set to
 * point to the fat_mutex again, possibly leaving chains which of pegs
 * which do not originate at the skinny_mutex (these accounted for in
 * the fat_mutex's refcount, so the pegs on these chains still prevent
 * the fat_mutex being freed).  We refer to the chain connecting the
 * skinny_mutex to the fat_mutex as the primary chain, and the others
 * as secondary chains, e.g.:
 *
 *                    +--------+   +--------+
 *                    |   peg  |   |   peg  |
 *  Secondary chain:  +--------+   +--------|
 *                    | next *---->| next *-------\
 *                    |   ...  |   |   ...  |      \
 *                    +--------+   +--------+      |
 *                                                 |
 *                          Primary chain:         v
 * +--------------+   +--------+   +--------+   +-----------+
 * | skinny_mutex |   |   peg  |   |   peg  |   | fat_mutex |
 * +--------------+   +--------+   +--------|   +-----------+
 * | val  *---------->| next *---->| next *---->|   ...     |
 * +--------------+   |   ...  |   |   ...  |   +-----------+
 *                    +--------+   +--------+      ^
 *                                                 |
 *                                 +--------+      |
 *                                 |   peg  |      |
 *                                 +--------|      /
 *               Secondary chain:  | next *-------/
 *                                 |   ...  |
 *                                 +--------+
 */

struct peg {
	struct common common;

	/* The refcount on this peg.  The peg can be freed when this
	   falls to 0.  This never exceeds 2, so we only need a byte. */
	uint8_t refcount;

	/* The next peg in the chain, or the fat_mutex at the end of
	   the chain. */
	struct common *next;
};


/* Given a skinny_mutex containing a pointer, find the associated
 * fat_mutex and lock its mutex.
 *
 * "skinny" points to the skinny_mutex.
 *
 * "p" is the pointer previously obtained from the skinny_mutex.
 *
 * "fatp" is used to return the pointer to the locked fat_mutex.
 *
 * Returns 0 on success, a positive error code, or <0 if the
 * skinny_mutex was found to no longer contain a pointer.
 */
static int fat_mutex_peg(skinny_mutex_t *skinny, struct common *p,
			 struct fat_mutex **fatp)
{
	int res;
	unsigned int peg_refcount_decr;
	struct fat_mutex *fat;
	struct peg *peg = malloc(sizeof *peg);
	if (!peg)
		return ENOMEM;

	/* Install our peg.  The initial ref count is two: One for the
	 * reference from this thread, and one that will be from the
	 * skinny_mutex. */
	peg->common.peg = 1;
	peg->refcount = 2;
	peg->next = p;

	while (!cas(&skinny->val, p, peg)) {
		/* The value in the skinny_mutex has changed from what
		   we saw earlier. */

		p = skinny->val;
		if ((uintptr_t)p <= 1) {
			/* There is no longer a fat_mutex to peg, so
			   backtrack. */
			free(peg);
			return -1;
		}

		/* There is a new fat_mutex, so try again to install
		   our peg. */
		peg->next = p;
	}

	/* Our peg is now installed.  Now we know the rest of the
	 * chain won't disappear under us, so we can walk it to find
	 * the fat_mutex and lock it. */
	while (p->peg)
		p = ((struct peg *)p)->next;

	*fatp = fat = (struct fat_mutex *)p;
	res = pthread_mutex_lock(&fat->mutex);

	/* The fat_mutex is locked, and we know it won't go away while
	 * we hold is lock.  So we can release our peg.
	 *
	 * To do this, we set the skinny_mutex to point to the
	 * fat_mutex, eliminating the primary chain.  As this thread's
	 * peg might not have been the only on of the primary chain,
	 * this might mean we create a new secondary chain
	 * (incrementing the fat_mutex's refcoutn accordingly).
	 * Handling the various cases correctly hinges on the
	 * refcounts.  By the end of this function, the fat_mutex
	 * refcount can be incremented, decremented, or returned to
	 * its original value. */
	p = atomic_xchg(&skinny->val, fat);

	/* By setting the skinny_mutex to point to the fat_mutex, we
	 * have theoretically created a new reference to it.  This
	 * might be a real reference (e.g. from a new secondary chain)
	 * or not.  If not, we'll decrement the fat_mutex refcount
	 * below. */
	fat->refcount++;

	/* In this loop, we walk the peg chain starting with the old
	 * value of skinny_mutex. */
	for (;;) {
		struct peg *chain_peg;

		peg_refcount_decr = 2;
		if (p == &peg->common)
			/* We have reached our peg, so fall through to
			 * the loop below. */
			break;

		peg_refcount_decr = 1;
		if (p == &fat->common) {
			/* We have reached the fat_mutex at the end of
			   the chain, eliminating a reference to it. */
			fat->refcount--;
			break;
		}

		/* Decrement the ref count of the peg, and see whether
		 * we can free it yet. */
		chain_peg = (struct peg *)p;
		if (atomic_sub_and_test(&chain_peg->refcount, 1))
			/* We can't free this peg yet, so leave a
			 * secondary chain in place. */
			break;

		/* Free the peg, and proceed to the next peg in the
		 * chain. */
		p = chain_peg->next;
		free(chain_peg);
	}

	for (;;) {
		if (atomic_sub_and_test(&peg->refcount, peg_refcount_decr))
			/* We can't free this peg yet, so leave a
			 * secondary chain in place. */
			break;

		/* No references to the peg remain, so free it. */
		p = peg->next;
		free(peg);

		if (p == &fat->common) {
			/* We have reached the fat_mutex at the end of
			   the chain, eliminating a reference to it. */
			fat->refcount--;
			break;
		}

		/* Proceed to the next peg in the chain. */
		peg = (struct peg *)p;
		peg_refcount_decr =  1;
	}

	return res;
}

/* Allocate a fat_mutex and associate it with a skinny_mutex.
 *
 * "skinny" points to the skinny_mutex.
 *
 * "head" is the value previously obtained from the skinny_mutex.
 *
 * "fatp" is used to return the pointer to the locked fat_mutex.
 *
 * Returns 0 on success, a positive error code, or <0 if the
 * skinny_mutex was found to no longer contain "head".
 */
static int skinny_mutex_promote(skinny_mutex_t *skinny, void *head,
				struct fat_mutex **fatp)
{
	int res = ENOMEM;
	struct fat_mutex *fat = malloc(sizeof *fat);
	*fatp = fat;
	if (!fat)
		goto err;

	fat->common.peg = 0;
	fat->held = !!head;
	/* If the skinny_mutex is held, then refcount needs to account
	   for the pseudo-reference from the holding thread. */
	fat->refcount = fat->held;
	fat->waiters = 0;

	res = pthread_mutex_init(&fat->mutex, NULL);
	if (res)
		goto err_mutex_init;

	res = pthread_cond_init(&fat->held_cond, NULL);
	if (res)
		goto err_cond_init;

	res = pthread_mutex_lock(&fat->mutex);
	if (res)
		goto err_mutex_lock;

	/* The fat_mutex is now ready, so try to make the skinny_mutex
	   point to it. */
	if (cas(&skinny->val, head, fat))
		return 0;

	res = -1;
	pthread_mutex_unlock(&fat->mutex);
 err_mutex_lock:
	pthread_cond_destroy(&fat->held_cond);
 err_cond_init:
	pthread_mutex_destroy(&fat->mutex);
 err_mutex_init:
	free(fat);
 err:
	return res;
}

/* Get and lock the fat_mutex associated with a skinny_mutex,
 * allocating it if necessary.
 *
 * "skinny" points to the skinny_mutex.
 *
 * "head" is the value that previously seen in the skinny_mutex.
 *
 * "fatp" is used to return the pointer to the locked fat_mutex.
 *
 * Returns 0 on success, a positive error code, or <0 if the
 * skinny_mutex value changed so that the operation should be retried.
 */
static int fat_mutex_get(skinny_mutex_t *skinny, struct common *head,
			 struct fat_mutex **fatp)
{
	if ((uintptr_t)head <= 1)
		return skinny_mutex_promote(skinny, head, fatp);
	else
		return fat_mutex_peg(skinny, head, fatp);
}

/* Decrement the refcount on a fat_mutex, unlock it, and free it if
   the conditions are right. */
static int fat_mutex_release(skinny_mutex_t *skinny, struct fat_mutex *fat)
{
	int keep, res;

	assert(!fat->held);

	/* If the decremented refcount reaches zero, then we know
	   there are no secondary peg chains or other threads pinning
	   the fat_mutex.  And if the skinny_mutex points to the
	   fat_mutex, then we know that there are no pegs on the
	   primary chain either.  So if the CAS succeeds in nulling
	   out the skinny_mutex, we can free the fat_mutex. */
	keep = (--fat->refcount || !strict_cas(&skinny->val, fat, NULL));

	res = pthread_mutex_unlock(&fat->mutex);
	if (keep || res)
		return res;

	res = pthread_mutex_destroy(&fat->mutex);
	if (res)
		return res;

	res = pthread_cond_destroy(&fat->held_cond);
	if (res)
		return res;

	free(fat);
	return 0;
}

/* Try to acquire a skinny_mutex with an associated fat_mutex.
 *
 * The fat_mutex's mutex will be released, so the calling thread
 * should already be accounted for in the fat_mutex's refcount.
 */
static int fat_mutex_lock(skinny_mutex_t *skinny, struct fat_mutex *fat)
{
	if (fat->held) {
		/* The mutex is already held, so we have to wait for
		 * it. */
		fat->waiters++;

		do {
			int res = pthread_cond_wait(&fat->held_cond,
						    &fat->mutex);
			if (res) {
				int res2;

				fat->waiters--;
				res2 = fat_mutex_release(skinny, fat);
				if (res2)
					return res2;

				return res;
			}
		} while (fat->held);

		fat->waiters--;
	}

	fat->held = 1;
	return pthread_mutex_unlock(&fat->mutex);
}

/* Called from skinny_mutex_lock when the fast path fails. */
int skinny_mutex_lock_slow(skinny_mutex_t *skinny)
{
	for (;;) {
		struct common *head = skinny->val;
		if (head) {
			struct fat_mutex *fat;
			int res = fat_mutex_get(skinny, head, &fat);
			if (!res) {
				fat->refcount++;
				return fat_mutex_lock(skinny, fat);
			}
			else if (res >= 0) {
				return res;
			}

			/* If fat_mutex_get returns a negative value,
			 * try again. */
		}
		else {
			/* Recapitulate skinny_mutex_lock */
			if (cas(&skinny->val, head, (void *)1))
				return 0;
		}
	}
}

/* Get and lock the fat_mutex associated with a skinny_mutex, when
 * this thread already holds the mutex. */
static int fat_mutex_get_held(skinny_mutex_t *skinny, struct fat_mutex **fatp)
{
	for (;;) {
		int res;
		struct common *head = skinny->val;
		if (!head)
			return EPERM;

		res = fat_mutex_get(skinny, head, fatp);
		if (res == 0)
			return (*fatp)->held ? 0 : EPERM;
		else if (res >= 0)
			return res;
	}
}

/* Called from skinny_mutex_unlock when the fast path fails. */
int skinny_mutex_unlock_slow(skinny_mutex_t *skinny)
{
	struct fat_mutex *fat;
	int res = fat_mutex_get_held(skinny, &fat);
	int res2;

	if (res)
		return res;

	fat->held = 0;
	res = 0;
	if (fat->waiters)
		/* Wake a single waiter. */
		res = pthread_cond_signal(&fat->held_cond);

	res2 = fat_mutex_release(skinny, fat);
	return !res2 ? res : res2;
}

int skinny_mutex_cond_timedwait(pthread_cond_t *cond, skinny_mutex_t *skinny,
				const struct timespec *abstime)
{
	struct fat_mutex *fat;
	int res = fat_mutex_get_held(skinny, &fat);
	if (res)
		return res;

	/* We will release the lock, so wake a waiter */
	if (fat->waiters) {
		res = pthread_cond_signal(&fat->held_cond);
		if (res) {
			pthread_mutex_unlock(&fat->mutex);
			return res;
		}
	}

       /* Relinquish the mutex.  But we leave our reference accounted
          for in fat->refcount in place, in order to pin the
          fat_mutex. */
	fat->held = 0;

	if (!abstime)
		res = pthread_cond_wait(cond, &fat->mutex);
	else
		res = pthread_cond_timedwait(cond, &fat->mutex, abstime);

	if (!res || res == ETIMEDOUT) {
		int res2 = fat_mutex_lock(skinny, fat);
		return res2 ? res2 : res;
	}

	/* The pthreads spec says that errors are reported as
	 * though the mutex was not dropped. */
	assert(!fat->held);
	fat->held = 1;
	pthread_mutex_unlock(&fat->mutex);
	return res;
}

int skinny_mutex_cond_wait(pthread_cond_t *cond, skinny_mutex_t *skinny)
{
	return skinny_mutex_cond_timedwait(cond, skinny, NULL);
}
