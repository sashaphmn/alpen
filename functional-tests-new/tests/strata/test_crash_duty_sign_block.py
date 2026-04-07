"""Test sequencer recovers after crash during block signing duty."""

import logging

import flexitest

from common.base_test import StrataNodeTest
from common.config import ServiceType

logger = logging.getLogger(__name__)


@flexitest.register
class TestCrashDutySignBlock(StrataNodeTest):
    """Crash the sequencer at the duty_sign_block bail point and verify recovery."""

    def __init__(self, ctx: flexitest.InitContext):
        ctx.set_env("basic")

    def main(self, ctx):
        strata = self.get_service(ServiceType.Strata)
        rpc = strata.wait_for_rpc_ready(timeout=10)

        # Let chain produce a few blocks first
        strata.wait_for_additional_blocks(2, rpc)
        pre_crash_status = strata.get_sync_status(rpc)
        pre_crash_tip = pre_crash_status["tip"]
        pre_crash_height = pre_crash_tip["slot"]
        logger.info(f"Pre-crash height: {pre_crash_height}, blkid: {pre_crash_tip['blkid']}")

        # Arm the bail trigger — process will abort next time it hits the bail point
        rpc.debug_bail("duty_sign_block")

        # Wait for the process to actually die
        strata.wait_for_down(timeout=30)
        logger.info("Sequencer crashed as expected")

        # Restart
        strata.stop()  # bookkeeping
        strata.start()
        rpc = strata.wait_for_rpc_ready(timeout=20)

        # Verify chain progresses past the crash point
        strata.wait_for_block_height(pre_crash_height + 1, rpc, timeout=30)
        post_status = strata.get_sync_status(rpc)
        post_tip = post_status["tip"]

        assert post_tip["slot"] > pre_crash_height, (
            f"Chain did not progress: {post_tip['slot']} <= {pre_crash_height}"
        )

        # Verify finalized epoch didn't regress
        pre_fin_epoch = pre_crash_status.get("finalized", {}).get("epoch", 0)
        post_fin_epoch = post_status.get("finalized", {}).get("epoch", 0)
        if pre_fin_epoch > 0:
            assert post_fin_epoch >= pre_fin_epoch, (
                f"Finalized epoch regressed: {post_fin_epoch} < {pre_fin_epoch}"
            )

        logger.info(f"Post-recovery height: {post_tip['slot']}, blkid: {post_tip['blkid']}")
        return True
