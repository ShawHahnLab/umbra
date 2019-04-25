# Common helper functions for specific demo bash scripts.

set -o errexit

# Adapted from:
# https://github.com/sunbeam-labs/sunbeam/blob/dev/tests/run_tests.bash
function cleanup {
	local RETCODE=$?
	if [[ $RETCODE -ne 0 ]]; then
		echo "$0 Failed"
	else
		echo "$0 OK"
	fi
	exit $RETCODE
}

# Run umbra with no system config (/etc/umbra.yml), flushing stdout/stderr
# after each line, using the python module from right here, with a given
# demo-templated config and seq root dir.
function run_umbra {
	# stdbuf: https://stackoverflow.com/a/30845184/4499968
	local seqroot=$1
	local config=$2
	UMBRA_SYSTEM_CONFIG="" stdbuf -oL -eL python -m umbra -c <(sed s/SEQROOT/$seqroot/g $config) -a process &
	sleep 1
	kill -s INT %1
	wait
}

trap cleanup exit
