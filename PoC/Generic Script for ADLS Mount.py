# Databricks notebook source
dbutils.widgets.text("storageaccount", "audeusdlsdevkcwgaap01")  #audeusdlsdevkcwgaap01
dbutils.widgets.text("containername", "") 

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

storageaccount = dbutils.widgets.get("storageaccount") 
containername = dbutils.widgets.get("containername")
required_mount_point = f'/mnt/{storageaccount}-{containername}'

# COMMAND ----------

# DBTITLE 1,Check if the required MountPoint is mounted or not

listOfMountPoints_withabfss = [item.mountPoint for item in dbutils.fs.mounts() if item.source.startswith('abfss')]
df_mounts = spark.createDataFrame(dbutils.fs.mounts())

if not(required_mount_point in listOfMountPoints_withabfss):
  print(required_mount_point + " is not mounted")
else:
  print(required_mount_point + " is already mounted")
  filtered_mounts = df_mounts.filter(df_mounts.mountPoint == required_mount_point)
  display(filtered_mounts)

# COMMAND ----------

# DBTITLE 1,Run to Unmount the inputted Path
dbutils.fs.unmount(required_mount_point)

# COMMAND ----------

# DBTITLE 1,Defining the fuction for Mounting the ADLS
def mountADLSUsingServicePrincipal(analysisID, storageAccountName, gl_MountPoint):
    try:
        listOfMountPoints = [item.mountPoint for item in dbutils.fs.mounts()]
        
        if not(gl_MountPoint in listOfMountPoints):                
            #if (key.strip() == ""):               
            print('Mounting using key vault and secret.')
            # Application (Client) ID
            applicationId = dbutils.secrets.get(scope="rules-scope",key="ClientId")
            # Application (Client) Secret Key
            authenticationKey = dbutils.secrets.get(scope="rules-scope",key="ClientSecret")
            # Directory (Tenant) ID
            tenantId = dbutils.secrets.get(scope="rules-scope",key="TenantId")

            endpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"
            source = "abfss://" + analysisID + "@" + storageAccountName + ".dfs.core.windows.net/"

            # Connecting using Service Principal secrets and OAuth
            configs = {"fs.azure.account.auth.type": "OAuth",
                        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id": applicationId,
                        "fs.azure.account.oauth2.client.secret": authenticationKey,
                        "fs.azure.account.oauth2.client.endpoint": endpoint}

            dbutils.fs.mount(
            source = source,
            mount_point = gl_MountPoint,
            extra_configs = configs)

        else:
            print("ADLS: " + storageAccountName + " is already mounted with the same mount point name:" + gl_MountPoint)

    except Exception as err:
        raise err

# COMMAND ----------

# DBTITLE 1,Executing the method for Mounting again
analysisID = containername
storageAccountName = storageaccount
gl_MountPoint = required_mount_point

mountADLSUsingServicePrincipal(analysisID, storageAccountName, gl_MountPoint)

# COMMAND ----------

# DBTITLE 1,Check if the below command is giving the list of folder in your container Root Path
try:
  if(dbutils.fs.ls(required_mount_point)): 
    print(f"Mount point is Accessible now. Please try accessing a sample file using the mount point: {required_mount_point}")
except:
  print("Mount point is not ready. Need to debug further")
