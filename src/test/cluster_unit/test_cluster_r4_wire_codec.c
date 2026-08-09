/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_wire_codec.c
 *    Stage 8 R4 exact request80/FORWARD96 extension codec tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_wire_codec.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void
run_wire_vector(int vector)
{
	ClusterR4RequestExtension request;
	ClusterR4ForwardExtension forward;
	uint64 master_generation = 0;
	uint64 transition_count = 0;
	SCN scn = InvalidScn;
	uint8 bytes[8];
	uint64 value;

	memset(&request, 0, sizeof(request));
	memset(&forward, 0, sizeof(forward));
	memset(bytes, 0, sizeof(bytes));

	switch (vector) {
		case 0:
			UT_ASSERT_EQ(sizeof(request), 16);
			break;
		case 1:
			UT_ASSERT_EQ(sizeof(forward), 32);
			break;
		case 2:
			UT_ASSERT_EQ(offsetof(ClusterR4ForwardExtension,
							  kind.cr.master_resource_transition_count_le), 12);
			break;
		case 3:
			UT_ASSERT_EQ(CLUSTER_R4_WIRE_VERSION, 1);
			break;
		case 4:
			UT_ASSERT_EQ(CLUSTER_R4_FORWARD_EXTENDED, 6);
			break;
		case 5:
			UT_ASSERT_EQ(CLUSTER_R4_WIRE_CR_BUILD, 1);
			break;
		case 6:
			UT_ASSERT(ClusterR4RequestExtensionSetCr(&request, (SCN)0x1234));
			UT_ASSERT(ClusterR4RequestExtensionGetCr(&request, &scn));
			UT_ASSERT_EQ((uint64)scn, UINT64_C(0x1234));
			break;
		case 7:
			UT_ASSERT(!ClusterR4RequestExtensionSetCr(NULL, (SCN)1));
			break;
		case 8:
			UT_ASSERT(!ClusterR4RequestExtensionSetCr(&request, InvalidScn));
			break;
		case 9:
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(NULL, &scn));
			break;
		case 10:
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, NULL));
			break;
		case 11:
			ClusterR4RequestExtensionSetCr(&request, (SCN)1);
			request.r4_version++;
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
			break;
		case 12:
			ClusterR4RequestExtensionSetCr(&request, (SCN)1);
			request.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
			break;
		case 13:
			ClusterR4RequestExtensionSetCr(&request, (SCN)1);
			request.flags_le[0] = 1;
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
			break;
		case 14:
			ClusterR4RequestExtensionSetCr(&request, (SCN)1);
			request.flags_le[1] = 1;
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
			break;
		case 15:
			ClusterR4RequestExtensionSetCr(&request, (SCN)1);
			request.reserved[3] = 1;
			UT_ASSERT(!ClusterR4RequestExtensionGetCr(&request, &scn));
			break;
		case 16:
			ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(9) << 32) | 4,
											 UINT64_C(7), (SCN)0x5678);
			UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, 9, &master_generation,
														  &transition_count, &scn));
			UT_ASSERT(master_generation == ((UINT64_C(9) << 32) | 4));
			UT_ASSERT(transition_count == UINT64_C(7));
			UT_ASSERT((uint64)scn == UINT64_C(0x5678));
			break;
		case 17:
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(NULL, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 18:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, NULL,
														   &transition_count, &scn));
			break;
		case 19:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation, NULL,
														   &scn));
			break;
		case 20:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, NULL));
			break;
		case 21:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			forward.r4_version++;
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 22:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			forward.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 23:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			forward.flags_le[0] = 1;
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 24:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			forward.subject_id_le[0] = 1;
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 25:
			ClusterR4ForwardExtensionSetCrProof(&forward, 0, 1, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 26:
			ClusterR4ForwardExtensionSetCrProof(&forward, UINT64_C(1) << 32, 1, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 1, &master_generation,
														   &transition_count, &scn));
			break;
		case 27:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 0, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 28:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, UINT64_MAX, InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														   &transition_count, &scn));
			break;
		case 29:
			ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(2) << 32) | 1, 1,
											 InvalidScn);
			UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&forward, 1, &master_generation,
														   &transition_count, &scn));
			break;
		case 30:
			ClusterR4ForwardExtensionSetCrProof(&forward, 1, 1, InvalidScn);
			UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, 0, &master_generation,
														  &transition_count, &scn));
			UT_ASSERT_EQ((uint64)scn, (uint64)InvalidScn);
			break;
		case 31:
			ClusterR4ForwardExtensionSetCrProof(&forward, (UINT64_C(0xffffffff) << 32) | 1, 1,
											 InvalidScn);
			UT_ASSERT(ClusterR4ForwardExtensionGetCrProof(&forward, UINT64_MAX,
														  &master_generation, &transition_count, &scn));
			break;
		default:
			value = UINT64_C(0x0102030405060708) ^ ((uint64)vector << 33)
					^ ((uint64)vector * UINT64_C(0x9e3779b97f4a7c15));
			ClusterR4WireWriteU64(bytes, value);
			UT_ASSERT(ClusterR4WireReadU64(bytes) == value);
			break;
	}
}

#define DEFINE_WIRE_TEST(n) \
	UT_TEST(test_wire_vector_##n) { run_wire_vector(n); }

DEFINE_WIRE_TEST(0)
DEFINE_WIRE_TEST(1)
DEFINE_WIRE_TEST(2)
DEFINE_WIRE_TEST(3)
DEFINE_WIRE_TEST(4)
DEFINE_WIRE_TEST(5)
DEFINE_WIRE_TEST(6)
DEFINE_WIRE_TEST(7)
DEFINE_WIRE_TEST(8)
DEFINE_WIRE_TEST(9)
DEFINE_WIRE_TEST(10)
DEFINE_WIRE_TEST(11)
DEFINE_WIRE_TEST(12)
DEFINE_WIRE_TEST(13)
DEFINE_WIRE_TEST(14)
DEFINE_WIRE_TEST(15)
DEFINE_WIRE_TEST(16)
DEFINE_WIRE_TEST(17)
DEFINE_WIRE_TEST(18)
DEFINE_WIRE_TEST(19)
DEFINE_WIRE_TEST(20)
DEFINE_WIRE_TEST(21)
DEFINE_WIRE_TEST(22)
DEFINE_WIRE_TEST(23)
DEFINE_WIRE_TEST(24)
DEFINE_WIRE_TEST(25)
DEFINE_WIRE_TEST(26)
DEFINE_WIRE_TEST(27)
DEFINE_WIRE_TEST(28)
DEFINE_WIRE_TEST(29)
DEFINE_WIRE_TEST(30)
DEFINE_WIRE_TEST(31)
DEFINE_WIRE_TEST(32)
DEFINE_WIRE_TEST(33)
DEFINE_WIRE_TEST(34)
DEFINE_WIRE_TEST(35)
DEFINE_WIRE_TEST(36)
DEFINE_WIRE_TEST(37)
DEFINE_WIRE_TEST(38)
DEFINE_WIRE_TEST(39)
DEFINE_WIRE_TEST(40)
DEFINE_WIRE_TEST(41)
DEFINE_WIRE_TEST(42)
DEFINE_WIRE_TEST(43)
DEFINE_WIRE_TEST(44)
DEFINE_WIRE_TEST(45)
DEFINE_WIRE_TEST(46)
DEFINE_WIRE_TEST(47)
DEFINE_WIRE_TEST(48)
DEFINE_WIRE_TEST(49)
DEFINE_WIRE_TEST(50)
DEFINE_WIRE_TEST(51)
DEFINE_WIRE_TEST(52)
DEFINE_WIRE_TEST(53)
DEFINE_WIRE_TEST(54)
DEFINE_WIRE_TEST(55)
DEFINE_WIRE_TEST(56)
DEFINE_WIRE_TEST(57)
DEFINE_WIRE_TEST(58)
DEFINE_WIRE_TEST(59)
DEFINE_WIRE_TEST(60)
DEFINE_WIRE_TEST(61)
DEFINE_WIRE_TEST(62)
DEFINE_WIRE_TEST(63)

#define RUN_WIRE_TEST(n) UT_RUN(test_wire_vector_##n)

int
main(void)
{
	UT_PLAN(64);
	RUN_WIRE_TEST(0);
	RUN_WIRE_TEST(1);
	RUN_WIRE_TEST(2);
	RUN_WIRE_TEST(3);
	RUN_WIRE_TEST(4);
	RUN_WIRE_TEST(5);
	RUN_WIRE_TEST(6);
	RUN_WIRE_TEST(7);
	RUN_WIRE_TEST(8);
	RUN_WIRE_TEST(9);
	RUN_WIRE_TEST(10);
	RUN_WIRE_TEST(11);
	RUN_WIRE_TEST(12);
	RUN_WIRE_TEST(13);
	RUN_WIRE_TEST(14);
	RUN_WIRE_TEST(15);
	RUN_WIRE_TEST(16);
	RUN_WIRE_TEST(17);
	RUN_WIRE_TEST(18);
	RUN_WIRE_TEST(19);
	RUN_WIRE_TEST(20);
	RUN_WIRE_TEST(21);
	RUN_WIRE_TEST(22);
	RUN_WIRE_TEST(23);
	RUN_WIRE_TEST(24);
	RUN_WIRE_TEST(25);
	RUN_WIRE_TEST(26);
	RUN_WIRE_TEST(27);
	RUN_WIRE_TEST(28);
	RUN_WIRE_TEST(29);
	RUN_WIRE_TEST(30);
	RUN_WIRE_TEST(31);
	RUN_WIRE_TEST(32);
	RUN_WIRE_TEST(33);
	RUN_WIRE_TEST(34);
	RUN_WIRE_TEST(35);
	RUN_WIRE_TEST(36);
	RUN_WIRE_TEST(37);
	RUN_WIRE_TEST(38);
	RUN_WIRE_TEST(39);
	RUN_WIRE_TEST(40);
	RUN_WIRE_TEST(41);
	RUN_WIRE_TEST(42);
	RUN_WIRE_TEST(43);
	RUN_WIRE_TEST(44);
	RUN_WIRE_TEST(45);
	RUN_WIRE_TEST(46);
	RUN_WIRE_TEST(47);
	RUN_WIRE_TEST(48);
	RUN_WIRE_TEST(49);
	RUN_WIRE_TEST(50);
	RUN_WIRE_TEST(51);
	RUN_WIRE_TEST(52);
	RUN_WIRE_TEST(53);
	RUN_WIRE_TEST(54);
	RUN_WIRE_TEST(55);
	RUN_WIRE_TEST(56);
	RUN_WIRE_TEST(57);
	RUN_WIRE_TEST(58);
	RUN_WIRE_TEST(59);
	RUN_WIRE_TEST(60);
	RUN_WIRE_TEST(61);
	RUN_WIRE_TEST(62);
	RUN_WIRE_TEST(63);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
