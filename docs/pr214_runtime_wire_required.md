# Runtime wire still required

This branch includes the safe helper and tests. The next amendment must wire the helper into `ap_master_control.py` at the approved-plan metadata point.

The runtime wire must be reviewed carefully because `ap_master_control.py` is production flow. Until that amendment is made and audited, this PR remains HOLD.
