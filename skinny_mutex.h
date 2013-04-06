#include <pthread.h>
#include <errno.h>

typedef struct {
	void *val;
} skinny_mutex_t;

static __inline__ int skinny_mutex_init(skinny_mutex_t *m)
{
	m->val = 0;
	return 0;
}

static __inline__ int skinny_mutex_destroy(skinny_mutex_t *m)
{
	return !m->val ? 0 : EBUSY;
}

#define SKINNY_MUTEX_INITIALIZER = { 0 };

extern int skinny_mutex_lock_slow(skinny_mutex_t *m);

static __inline__ int skinny_mutex_lock(skinny_mutex_t *m)
{
	if (__builtin_expect(__sync_bool_compare_and_swap(&m->val,
							  (void *)0, (void *)1),
			     1))
		return 0;
	else
		return skinny_mutex_lock_slow(m);
}

extern int skinny_mutex_unlock_slow(skinny_mutex_t *m);

static __inline__ int skinny_mutex_unlock(skinny_mutex_t *m)
{
	if (__builtin_expect(__sync_bool_compare_and_swap(&m->val,
							  (void *)1, (void *)0),
			     1))
		return 0;
	else
		return skinny_mutex_unlock_slow(m);
}

extern int skinny_mutex_cond_wait(pthread_cond_t *cond, skinny_mutex_t *m);
extern int skinny_mutex_cond_timedwait(pthread_cond_t *cond, skinny_mutex_t *m,
				       const struct timespec *abstime);
