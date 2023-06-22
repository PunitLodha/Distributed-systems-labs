#function run
run() {
    number=$1
    shift
    for i in `seq $number`; do
        make clean
        make
        ./start.sh
        ./test-lab-4-a.pl ./yfs1
        ./stop.sh

        ./start.sh
        ./test-lab-4-b ./yfs1 ./yfs2
        ./stop.sh

        ./start.sh
        ./test-lab-4-c ./yfs1 ./yfs2
        ./stop.sh
    done
}

run $1