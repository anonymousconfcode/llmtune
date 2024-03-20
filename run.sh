#!/bin/sh
#SBATCH --comment=ruc_kr_427
#SBATCH --job-name=49_54
#SBATCH --nodes=1
#SBATCH --ntasks=24
#SBATCH --partition=cpu24c
#SBATCH --nodelist=cpu24c-14 
# source activate ogtune
# module load postgresql/12.2-gcc_13.1.0
# pg_ctl -D /fs/fast/u2023103707/tpch24$4  -l logfile start
# for ((i=$1;i<=$2;i++))
# do
#     python main.py --workload bike_share_1_$i.wg --host $3 --database bike_share_1 --datapath /fs/fast/u2023103707/tpch242
#     echo "$i workload finished!"
# done

workloads=("6.wg" "_retails_9.wg" "language_corpus_26.wg" "talkingdata_3.wg" "54.wg" "language_corpus_28.wg" "bike_share_1_47.wg" "bike_share_1_49.wg" "52.wg" "language_corpus_32.wg" "_movie_platform_16.wg" "language_corpus_31.wg" "_retails_6.wg")

databases=("tpch24" "_retails" "language_corpus" "talkingdata" "tpch24" "language_corpus" "bike_share_1" "bike_share_1" "tpch24" "language_corpus" "_movie_platform" "language_corpus" "_retails")

# workloads=("54.wg" "40.wg")


# for i in "${my_array[@]}"; do
#     python main.py --workload $i --host $1 --database  --datapath /fs/fast/u2023103707/tpch248
#     echo "$i workload finished!"
# done

for ((i=0; i<${#workloads[@]}; i++)); do
    python main.py --workload ${workloads[$i]} --host cpu24c-48 --database ${databases[$i]} --datapath /fs/fast/u2023103707/tpch244 --warmup workload_map --method HEBO
    echo "${workloads[$i]} workload for database ${databases[$i]} finished!"
done