#Start Apache Spark
start-master.sh
start-worker.sh 0.0.0.0:7077

#Start API server
uvicorn masschange.api.app:app --root-path $API_ROOT_PATH --host 0.0.0.0 --reload --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini