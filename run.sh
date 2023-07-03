#function run
run() {
    number=$1
    shift
    for i in `seq $number`; do
        rm -f *.log
        # make clean
        make
        ./rsm_tester.pl 0
        killall lock_server
    done
}

run $1