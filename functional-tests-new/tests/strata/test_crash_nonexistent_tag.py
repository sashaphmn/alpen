"""Negative test: a bail tag that matches no bail point should NOT crash the process."""

import logging

import flexitest

from common.base_test import StrataNodeTest
from common.config import ServiceType

logger = logging.getLogger(__name__)


@flexitest.register
class TestCrashNonexistentTag(StrataNodeTest):
    """Verify that arming a bail tag with no matching bail point does not crash."""

    def __init__(self, ctx: flexitest.InitContext):
        ctx.set_env("basic")

    def main(self, ctx):
        strata = self.get_service(ServiceType.Strata)
        rpc = strata.wait_for_rpc_ready(timeout=10)

        strata.wait_for_additional_blocks(2, rpc)
        pre_height = strata.get_cur_block_height(rpc)
        logger.info(f"Height before arming fake bail: {pre_height}")

        # Arm a tag that doesn't match any bail point
        rpc.debug_bail("nonexistent_tag_that_matches_nothing")

        # Process should stay alive and keep producing blocks
        strata.wait_for_additional_blocks(3, rpc)
        post_height = strata.get_cur_block_height(rpc)

        assert post_height >= pre_height + 3, (
            f"Process stopped producing blocks after non-matching bail: "
            f"{post_height} < {pre_height + 3}"
        )
        assert strata.check_status(), "Process died from non-matching bail tag"

        logger.info(f"Process alive, height: {post_height} — non-matching bail correctly ignored")
        return True
