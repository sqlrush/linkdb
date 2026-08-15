/*-------------------------------------------------------------------------
 *
 * pgrac_fenced.c
 *	  Privileged provider-neutral external-fence daemon skeleton.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <unistd.h>

#include "pgrac_fenced_config.h"

#define PGRAC_FENCED_EXIT_UNAVAILABLE 77

static bool
read_root_config(PgracFencedConfigV1 *config)
{
	uint8 *bytes;
	struct stat st;
	ssize_t got;
	size_t used = 0;
	int fd;
	bool ok = false;

	fd = pgrac_fenced_config_open_secure();
	if (fd < 0)
		return false;
	if (fstat(fd, &st) != 0 || !pgrac_fenced_config_stat_secure(&st))
		goto done;
	bytes = (uint8 *) malloc((size_t) st.st_size);
	if (bytes == NULL)
		goto done;
	while (used < (size_t) st.st_size)
	{
		got = read(fd, bytes + used, (size_t) st.st_size - used);
		if (got <= 0)
			goto free_done;
		used += (size_t) got;
	}
	ok = pgrac_fenced_config_parse_v1(bytes, used, config) ==
		PGRAC_FENCED_CONFIG_OK;

free_done:
	memset(bytes, 0, used);
	free(bytes);
done:
	(void) close(fd);
	return ok;
}

int
main(int argc, char **argv)
{
	PgracFencedConfigV1 *config;
	int rc = PGRAC_FENCED_EXIT_UNAVAILABLE;

	(void) argc;
	(void) argv;
	if (geteuid() != 0)
	{
		fprintf(stderr, "pgrac-fenced must run as root\n");
		return PGRAC_FENCED_EXIT_UNAVAILABLE;
	}
	config = (PgracFencedConfigV1 *) calloc(1, sizeof(*config));
	if (config == NULL || !read_root_config(config))
	{
		fprintf(stderr, "pgrac-fenced configuration is unavailable\n");
		goto done;
	}
	/* No provider is selected in the current approved package.  Provider 0,
	 * and every unregistered nonzero id, stays fail-closed without a socket. */
	fprintf(stderr, "pgrac-fenced has no certified production provider\n");
done:
	if (config != NULL)
	{
		memset(config, 0, sizeof(*config));
		free(config);
	}
	return rc;
}
