# EXIT_WORKER.PY — DISABLED
# This file is dead code. The exit logic is fully owned by:
#   ClientRunner → APExecutionCore → APExitEngine
# It is NOT referenced in any Procfile or render.yaml and must NOT be run.
# Left in repo for git history. Do not import or execute.
#
# Original content below (commented out):
#
# # exit_worker.py
# from ap.exit_manager import exit_manager_loop
# from ap.client_manager import get_client_broker
# from ap.logger import get_logger
# 
# log = get_logger("exit_worker")
# 
# if __name__ == "__main__":
#     log.info("📊 Exit manager starting...")
#     broker = get_client_broker("default")
#     exit_manager_loop(broker, poll_seconds=10)