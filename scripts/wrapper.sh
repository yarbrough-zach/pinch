#!/bin/bash
source /cvmfs/oasis.opensciencegrid.org/ligo/sw/conda/etc/profile.d/conda.sh
conda activate igwn-py39
python3 /ligo/home/ligo.org/zach.yarbrough/TGST/observing/4/a/tgst/scripts/get_gstlal_triggers_preallocate.py $@
