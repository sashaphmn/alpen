"""Test sequencer recovers after crash during fork choice new block processing."""

import logging

import flexitest

from common.base_test import StrataNodeTest
from common.config import ServiceType

logger = logging.getLogger(__name__)


@flexitest.register
class TestCrashFcmNewBlock(StrataNodeTest):
    """Crash at fcm_new_block bail point and verify recovery."""

    def __init__(self, ctx: flexitest.InitContext):
        ctx.set_env("basic")

    def main(self, ctx):
        strata = self.get_service(ServiceType.Strata)
        rpc = strata.wait_for_rpc_ready(timeout=10)

        strata.wait_for_additional_blocks(2, rpc)
        pre_crash_status = strata.get_sync_status(rpc)
        pre_crash_height = pre_crash_status["tip"]["slot"]
        logger.info(f"Pre-crash height: {pre_crash_height}")

        rpc.debug_bail("fcm_new_block")

        strata.wait_for_down(timeout=30)
        logger.info("Sequencer crashed as expected")

        strata.stop()
        strata.start()
        rpc = strata.wait_for_rpc_ready(timeout=20)

        # FCM crash happens during block processing — verify recovery with 2 blocks
        strata.wait_for_block_height(pre_crash_height + 2, rpc, timeout=30)
        post_status = strata.get_sync_status(rpc)

        assert post_status["tip"]["slot"] > pre_crash_height + 1

        # Verified confirmed epoch didn't regress
        pre_confirmed = pre_crash_status.get("confirmed", {}).get("epoch", 0)
        post_confirmed = post_status.get("confirmed", {}).get("epoch", 0)
        assert post_confirmed >= pre_confirmed, (
            f"Confirmed epoch regressed: {post_confirmed} < {pre_confirmed}"
        )

        logger.info(f"Post-recovery height: {post_status['tip']['slot']}")
        return True
