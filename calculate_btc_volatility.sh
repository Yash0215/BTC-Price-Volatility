spark-submit \
    --name "Calculate BTC price volatility" \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 1g \
    --num-executors 1 \
    --executor-memory 1g \
    --py-files "src.zip" \
    --files config.ini
    src/compute_btc_volatility.py
