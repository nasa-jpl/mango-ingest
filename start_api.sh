mkdir -p /logs/api

#Start API server
if [[ "${API_ENABLE_SSL:-0}" != "0" ]]; then
  python -u -m uvicorn masschange.api.app:app \
    --root-path "$API_ROOT_PATH" \
    --host 0.0.0.0 \
    --ssl-keyfile=/certs/ssl.key \
    --ssl-certfile=/certs/ssl.crt \
    --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini
else
  python -u -m uvicorn masschange.api.app:app \
    --root-path "$API_ROOT_PATH" \
    --host 0.0.0.0 \
    --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini
fi