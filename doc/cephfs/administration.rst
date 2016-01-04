
CephFS Administrative commands
==============================

Filesystems
-----------

fs new

fs ls

fs rm <filesystem name> [--yes-i-really-mean-it]

fs reset <filesystem name>

fs get <filesystem name>

fs set <filesystem name> <var> <val>

fs dump

#TODO add/rm data pool
#TODO compat show/rm compat/incompat
#TODO repaired


Daemons
-------

fail
deactivate
tell
metadata


Legacy
======

mds stat
mds dump
mds getmap
mds compat show
mds stop
mds set_max_mds # replaced by "fs set max_mds"
mds set # replaced by "fs set"
mds setmap
mds set_state
mds repaired  # replaced by TODO "fs repaired"
mds rmfailed
mds cluster_down  # replaced by "fs set cluster_down"
mds cluster_up
mds compat rm_compuat # replaced by TODO fs compat
mds compat rm_incompat # replaced by TODO fs compat
mds newfs # replaced by "fs new"
mds add_data_pool # replaced by TODO
mds remove_data_pool #replaced by TODO


