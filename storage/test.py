from storage import Storage

account_name = "<Your Azure storage account name>"
account_key = "<Your Azure storage account key>"
container_name = "<The container you want to connect>"


client = Storage(account_name=account_name,account_key=account_key,container_name=container_name)

"""
You can use this client to interact with your storage container.
"""