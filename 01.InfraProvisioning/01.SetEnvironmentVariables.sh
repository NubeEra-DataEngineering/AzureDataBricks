#!/bin/bash
# https://vscode.dev/
# 0. Configure Environment Variables
####################################
POSTFIX_PATTERN="304"
AZ_LOCATION="eastus"

AZ_RG_NAME="rg"$POSTFIX_PATTERN
AZ_IDENTITY_NAME="azidentity"$POSTFIX_PATTERN
AZ_DBWS_NAME="azdbws"$POSTFIX_PATTERN
AZ_STORAGE_ACCOUNT_NAME="azsa"$POSTFIX_PATTERN
AZ_CONTAINER_NAME="metastore"
AZ_DB_ACCESS_CONNECTER_NAME="access-connector"$POSTFIX_PATTERN