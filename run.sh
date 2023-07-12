#function run
run() {
    number=$1
    shift
    for i in `seq $number`; do
        rm -f *.log
        # make clean
        make
        ./rsm_tester.pl 0 1 2 3 4 5 6 7
        killall lock_server
    done
}

run $1