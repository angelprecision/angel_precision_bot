# PR 213 safety boundary

This branch is diagnostics only. It must not change execution behavior. It must not submit or cancel broker orders. It must not mutate queue, order, position, or proof records. It must not replace scanner score or plan score.
