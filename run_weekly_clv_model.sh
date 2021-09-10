#!/bin/bash
# exec 3>&1 4>&2
# trap 'exec 2>&4 1>&3' 0 1 2 3
# exec 1>backtest.out 2>&1
# exit when any command fails
# set -e


datediff() {
    d1=$(date -d "$1" +%s)
    d2=$(date -d "$2" +%s)
    local count=$(( (d1 - d2) / 86400/7))
    echo "$count"
}

function run_model(){
    for p in week month
    do
        train_file_name=train-data-${p}-${model_name}-${YYYYWW}.gz
        predict_file_name=predict-data-${p}-${model_name}-${YYYYWW}.gz
        predictions_file_name=predictions-${p}-${model_name}-${YYYYWW}.csv
        consolidated_file=predictions-${p}-${model_name}-${YYYYWW}_consolidated.csv

        view_train="$(sed -e 's/\${day}/'${start}'/' \
                    /home/pmeadm/clv-cpa-retention-curves/queries/query_template_train_${p}.sql)"

        view_predict="$(sed -e 's/\${day}/'${start}'/' -e 's/\${YYYYWW}/'${YYYYWW}'/' -e 's/\${YYYYWWs}/'${YYYYWWs}'/' \
                    /home/pmeadm/clv-cpa-retention-curves/queries/query_template_predict_${p}.sql)"

        echo `date`[${p}ly subs: running queries on clickhouse...]

        python3 /home/pmeadm/clv-cpa-retention-curves/dump_data.py \
            "$train_file_name" "$predict_file_name" \
            "$view_train" "$view_predict" \

        echo `date`[${p}ly subs: queries executed and data upload ok.]

        # docker run -v /models:/models  -v /data:/data  --rm --name clv_backtest docker.jamba.net/bi/clv-cpa-survival-curves:late-bill_118650 \
        #     /bin/bash -c "\
        #     python3 app.py train \
        #         /data/${train_file_name} \
        #         /models/${model_name}-${YYYYWW}  week; \
        #     python3 app.py predict \
        #         /data/${predict_file_name} \
        #         /data/${predictions_file_name} \
        #         /models/${model_name}-${YYYYWW}  week ${model_name}"

        python3 /home/pmeadm/clv-cpa-retention-curves/app.py train \
            /data/${train_file_name} \
            /models/${model_name}-${YYYYWW}  ${p}
        python3 /home/pmeadm/clv-cpa-retention-curves/app.py predict \
            /data/${predict_file_name} \
            /data/${predictions_file_name} \
            /models/${model_name}-${YYYYWW}  ${p} ${model_name}

        echo `date`[formating weighted curves...]
        python3 /home/pmeadm/clv-cpa-retention-curves/weigthed_curves/clv-weighted-curves.py
        sh /home/pmeadm/clv-cpa-retention-curves/weigthed_curves/clickhouse_push.sh

        # cat /data/${consolidated_file} | clickhouse-client --query="INSERT INTO pme.clv_survival_curves_prediction_${p} FORMAT CSVWithNames"
    done
    }

model_name='final-curves'

if [ "$1" == "backtest" ]
    then
        mode=$1
        topDay=$(date -d "$2" +%Y-%m-%d)
        bottomDay=$(date -d "$topDay $(($3+1)) weeks ago" +%Y-%m-%d)
        topWeek=$(date -d "$topDay" +%Y%V)
        count_weeks=$(datediff $topDay $bottomDay)
        bottomWeek=$(date -d "$topDay $count_weeks weeks ago" +%Y%V)
        #diff=$(($topWeek-$bottomWeek))
        start=$topDay
        #for i in `seq $bottomWeek $topWeek`
        for i in `seq $(($count_weeks/4))`
        do
            #start=$(date -d "$topDay - $((7*$diff)) days" +%Y-%m-%d)
            stop=$(date -d "$start 3 weeks ago" +%Y-%m-%d)
            YYYYWW=$(date -d $start +%Y%V)
            YYYYWWs=$(date -d $stop +%Y%V)
            echo [Running model on $mode mode from week $bottomWeek to $topWeek - on week $YYYYWW -$YYYYWWs ]
            run_model
            start=$(date -d "$stop 1 weeks ago" +%Y-%m-%d)
            #count=$(($diff-1))
        done
    else
        echo Running clv survival curves prediction model
        start=$(date -d "last Sunday" +%Y-%m-%d)
        stop=$(date -d "$start 3 weeks ago" +%Y-%m-%d)
        YYYYWW=$(date -d $start +%Y%V)
        YYYYWWs=$(date -d $stop +%Y%V)
        echo start date: $start $YYYYWW stop date: $stop $YYYYWWs
        run_model
fi
