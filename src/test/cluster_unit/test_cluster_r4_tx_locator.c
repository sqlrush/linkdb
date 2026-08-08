/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_locator.c
 *	  R4 exact caller-selected transaction-locator tests.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_tx_resolve.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

static char test_page[BLCKSZ];

static Page
build_itl_page(void)
{
	PageHeader header;

	memset(test_page, 0, sizeof(test_page));
	header = (PageHeader)test_page;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_upper = header->pd_special;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	return (Page)test_page;
}

static ClusterItlSlotData *
slot_at(Page page, uint8 index)
{
	return &ClusterPageGetItlSlots(page)[index];
}

static bool
bytes_are_zero(const void *ptr, size_t size)
{
	const unsigned char *bytes = ptr;
	size_t i;

	for (i = 0; i < size; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

UT_TEST(test_frozen_identity_layout)
{
	UT_ASSERT_EQ(sizeof(ClusterUndoByteAddress), 16);
	UT_ASSERT_EQ(sizeof(ClusterTxLocator), 24);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, uba), 0);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, xid), 16);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, tt_wrap), 20);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, itl_kind), 22);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, itl_slot_index), 23);
	UT_ASSERT_EQ(sizeof(ClusterMultiResolutionMember), 24);
}

UT_TEST(test_frozen_closed_domains)
{
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_VISIBILITY, 0);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_CLEANOUT_HINT, 3);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_PROTOCOL, 25);
	UT_ASSERT_EQ(CLUSTER_TX_UNKNOWN, 0);
	UT_ASSERT_EQ(CLUSTER_TX_ABORTED, 4);
	UT_ASSERT_EQ(CLUSTER_TX_PROOF_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON, 7);
}

UT_TEST(test_reason_names_are_stable)
{
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_NONE), "none");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_TARGET_DISABLED),
					 "target_disabled");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_BAD_UBA), "bad_uba");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_XID_MISMATCH),
					 "xid_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_WRAP_MISMATCH),
					 "wrap_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_SLOT_MISMATCH),
					 "slot_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_PROTOCOL), "protocol");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name((ClusterTxResolveReason)26), "invalid_reason");
}

UT_TEST(test_exact_resolver_is_dormant_and_zeroes_output)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(NULL, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_multixact_resolver_is_dormant_and_zeroes_output)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)0, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_bad_locator_scaffold_fails_closed_and_zeroes_output)
{
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	memset(&locator, 0xA5, sizeof(locator));
	UT_ASSERT(!cluster_tx_locator_from_itl(NULL, 0, &locator, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
	UT_ASSERT(bytes_are_zero(&locator, sizeof(locator)));
}

UT_TEST(test_valid_caller_selected_data_slot_forms_exact_locator)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 3);
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_BAD_LOCATOR;

	slot->xid = (TransactionId)798;
	slot->wrap = 7;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head.raw[0] = ((uint64)408 << 32) | 11;
	slot->undo_segment_head.raw[1] = 5;

	UT_ASSERT(cluster_tx_locator_from_itl(page, 3, &locator, &reason));
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_frozen_identity_layout);
	UT_RUN(test_frozen_closed_domains);
	UT_RUN(test_reason_names_are_stable);
	UT_RUN(test_exact_resolver_is_dormant_and_zeroes_output);
	UT_RUN(test_multixact_resolver_is_dormant_and_zeroes_output);
	UT_RUN(test_bad_locator_scaffold_fails_closed_and_zeroes_output);
	UT_RUN(test_valid_caller_selected_data_slot_forms_exact_locator);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
