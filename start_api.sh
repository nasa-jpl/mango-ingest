#Start API server
uvicorn masschange.api.app:app --root-path "$API_ROOT_PATH" --host 0.0.0.0 --log-config "$MASSCHANGE_REPO_ROOT"/src/masschange/api/log.ini