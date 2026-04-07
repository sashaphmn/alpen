"""Test sequencer recovers after crash during CSM event processing."""

import logging

import flexitest

from common.base_test import StrataNodeTest
from common.config import ServiceType

logger = logging.getLogger(__name__)


@flexitest.register
class TestCrashCsmEvent(StrataNodeTest):
    """Crash at csm_event bail point and verify recovery.

    The CSM worker processes ASM status updates triggered by L1 block arrivals.
    We mine Bitcoin blocks after arming the bail to ensure the CSM path is hit.
    """

    def __init__(self, ctx: flexitest.InitContext):
        ctx.set_env("basic")

    def main(self, ctx):
        strata = self.get_service(ServiceType.Strata)
        bitcoin = self.get_service(ServiceType.Bitcoin)
        rpc = strata.wait_for_rpc_ready(timeout=10)

        strata.wait_for_additional_blocks(2, rpc)
        pre_crash_status = strata.get_sync_status(rpc)
        pre_crash_height = pre_crash_status["tip"]["slot"]
        logger.info(f"Pre-crash height: {pre_crash_height}")

        # Arm the bail trigger
        rpc.debug_bail("csm_event")

        # Mine L1 blocks to trigger ASM → CSM event processing
        btc_rpc = bitcoin.create_rpc()
        addr = btc_rpc.proxy.getnewaddress()
        btc_rpc.proxy.generatetoaddress(3, addr)

        strata.wait_for_down(timeout=30)
        logger.info("Sequencer crashed as expected")

        strata.stop()
        strata.start()
        rpc = strata.wait_for_rpc_ready(timeout=20)

        strata.wait_for_block_height(pre_crash_height + 1, rpc, timeout=30)
        post_status = strata.get_sync_status(rpc)

        assert post_status["tip"]["slot"] > pre_crash_height
        logger.info(f"Post-recovery height: {post_status['tip']['slot']}")
        return True
