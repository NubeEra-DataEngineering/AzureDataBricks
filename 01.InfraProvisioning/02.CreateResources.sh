# 1. Create Resource Group
###########################
az group create \
  --name $AZ_RG_NAME \
  --location $AZ_LOCATION

# 2. Create a Storage Account
#############################
az storage account create \
  --resource-group $AZ_RG_NAME \
  --location $AZ_LOCATION \
  --name $AZ_STORAGE_ACCOUNT_NAME \
  --sku Standard_LRS \
  --kind StorageV2 --hns

# 3. Create Container
#####################
az storage container create \
  --name $AZ_CONTAINER_NAME \
  --account-name $AZ_STORAGE_ACCOUNT_NAME

# 4. Create a User Assigned Managed Identity
############################################
az identity create \
  -g $AZ_RG_NAME \
  -n $AZ_IDENTITY_NAME


# 5. Create Role Assignment (Get Principal ID, Scope)
#####################################################
principalId=$(az identity show \
  --name $AZ_IDENTITY_NAME \
  --resource-group $AZ_RG_NAME \
  --query principalId \
  --output tsv \
)

scope=$(az storage account show \
  --name $AZ_STORAGE_ACCOUNT_NAME \
  --resource-group $AZ_RG_NAME \
  --query id --output tsv)

az role assignment create \
  --assignee $principalId \
  --role "Storage Blob Data Contributor" \
  --scope $scope

az role assignment create \
  --assignee $principalId \
  --role "Storage Queue Data Contributor" \
  --scope $scope

# 6. Get Principal Resource ID
#####################################
# Select Subscription--> Resource Providers --> Microsoft.ManagedIdentity --> Register

principalResourceId=$(az identity show \
  --resource-group $AZ_RG_NAME \
  --name $AZ_IDENTITY_NAME \
  --query id \
  --output tsv)

# ex: /subscriptions/<<SUBID>>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo

#7. Create Access Connector Resource
####################################
az databricks access-connector create \
  --resource-group $AZ_RG_NAME \
  --name $AZ_DB_ACCESS_CONNECTER_NAME \
  --location $AZ_LOCATION \
  --identity-type UserAssigned \
  --user-assigned-identities "{$principalResourceId}"

# 8. Get Access Connector ID & Identity ID 
##########################################
az databricks access-connector show \
  -n $AZ_DB_ACCESS_CONNECTER_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

#output 
# /subscriptions/<SUBID>/resourceGroups/rg-db-exo/providers/Microsoft.Databricks/accessConnectors/exo-db-access-connector
# /subscriptions/fca36345-619f-4ce5-a80e-61941df69f35/resourceGroups/rg304/providers/Microsoft.Databricks/accessConnectors/access-connector304

az identity show \
  --name $AZ_IDENTITY_NAME \
  --resource-group $AZ_RG_NAME \
  --query id

# Output
# /subscriptions/<SUBID>/resourcegroups/rg-db-exo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-db-exo
# /subscriptions/fca36345-619f-4ce5-a80e-61941df69f35/resourcegroups/rg304/providers/Microsoft.ManagedIdentity/userAssignedIdentities/azidentity304


# 9. Create Databricks Workspace
################################
az databricks workspace create \ 
  --resource-group $AZ_RG_NAME \ 
  --name $AZ_DBWS_NAME \ 
  --location $AZ_LOCATION \ 
  --sku premium