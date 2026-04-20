mkdir -p /logs/api

CIPHER_SUITE="ECDHE-ECDSA-AES256-GCM-SHA384"
CIPHER_SUITE+=":ECDHE-ECDSA-AES128-GCM-SHA256"
CIPHER_SUITE+=":ECDHE-ECDSA-AES256-SHA384"
CIPHER_SUITE+=":ECDHE-ECDSA-AES128-SHA256"
CIPHER_SUITE+=":ECDHE-RSA-AES256-GCM-SHA384"
CIPHER_SUITE+=":ECDHE-RSA-AES128-GCM-SHA256"
CIPHER_SUITE+=":ECDHE-RSA-AES256-SHA384"
CIPHER_SUITE+=":ECDHE-RSA-AES128-SHA256"
CIPHER_SUITE+=":!aNULL:!eNULL:!EXPORT:!DES:!RC4:!3DES:!MD5:!PSK"

if [[ "${API_ENABLE_SSL:-0}" != "0" ]]; then
  python -u -m uvicorn masschange.api.app:app \
    --root-path "$API_ROOT_PATH" \
    --host 0.0.0.0 \
    --ssl-keyfile=/certs/ssl.key \
    --ssl-certfile=/certs/ssl.crt \
    --ssl-ciphers="$CIPHER_SUITE" \
    --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini
else
  python -u -m uvicorn masschange.api.app:app \
    --root-path "$API_ROOT_PATH" \
    --host 0.0.0.0 \
    --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini
fi