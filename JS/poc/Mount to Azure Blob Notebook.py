# Databricks notebook source
#dbutils.fs.mount(
#    source = 'wasbs://<container_name>@<storage account name>.blob.core.windows.net/',
#    mount_point = '/mnt/blobstorage',
#    extra_configs = {'fs.azure.account.key.<storage account name>.blob.core.windows.net' : '<azure access key>'}
#)

# COMMAND ----------

#SS = dbutils.secrets.get('AzureCredentials' , 'StorageAccountName')
#print(SS)

# COMMAND ----------


storage_account_name = dbutils.secrets.get("AzureCredentials", "StorageAccountName")
container_name = dbutils.secrets.get("AzureCredentials", "ContainerName")
access_key = dbutils.secrets.get("AzureCredentials", "AccessKey")
source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
dbutils.fs.mount(
    source=source,
    mount_point="/mnt/blobstorage",
    extra_configs={"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": access_key}
)
