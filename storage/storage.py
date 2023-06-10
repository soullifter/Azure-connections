import os
from datetime import datetime, timedelta

from azure.storage.blob import (
    BlobSasPermissions,
    BlobServiceClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from utils import find


class Storage:
    """
    Storage class to interact with any Azure blob storage container, this class has the following methods.
        1. upload method which helps in uploading a file from local to a provided destination path in azure blob container.
        2. download method helps in downloading a file in azure to local.
    """

    def __init__(self, account_name, account_key, container_name):
        """
        To initialize the Storage class we need to provide the account_name and account_key of the azure storage container,
        and the respective container you want to connect to.
        """
        self._account_name = account_name
        self._account_key = account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        self._container_name = container_name

        self._blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=connection_string
        )

        self._container_client = self._blob_service_client.get_container_client(
            container=container_name
        )

        self._fs_prefix = (
            f"https://{self._account_name}.blob.core.windows.net/{self._container_name}"
        )

        if not self._container_client.exists():
            self._container_client.create_container()

    def upload(self, file, destination_path):
        """
        Uploads a file from local storage to the provided destination path in the container.
        """
        blob_client = self._container_client.get_blob_client(blob=destination_path)

        with open(file, "rb") as data:
            blob_client.upload_blob(data.read(), overwrite=True)

    def download(self, cloud_path, local_path):
        """
        downloads the given file in cloud to local.
        """
        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name, blob=cloud_path
        )

        # This returns a StorageStreamDownloader
        stream = blob_client.download_blob()
        with open(local_path, "wb+") as local_file:
            # Read data in chunks to avoid loading all into memory at once, helpful for bigger files.
            for chunk in stream.chunks():
                # Process your data (anything can be done here - 'chunk' is a byte array)
                local_file.write(chunk)

    def delete(self, destination_path):
        """
        deletes the file in azure storage.
        """
        blob = self._blob_service_client.get_blob_client(
            container=self._container_name, blob=destination_path
        )
        blob.delete_blob(delete_snapshots="include")

    def generate_sas_token(self, relative_path, minutes=60):
        """
        blob level sas token.
        """
        sas_permissions = BlobSasPermissions(read=True, write=True)
        sas_token = generate_blob_sas(
            account_name=self._account_name,
            container_name=self._container_name,
            blob_name=relative_path,
            account_key=self._account_key,
            permission=sas_permissions,
            # the link is valid for "minutes" min.
            expiry=datetime.utcnow() + timedelta(minutes=minutes),
        )

        return sas_token

    def generate_container_sas_token(
        self, relative_path, minutes, sas_permissions=BlobSasPermissions(read=True)
    ):
        """
        container level sas token.
        """
        sas_token = generate_container_sas(
            account_name=self._account_name,
            container_name=self._container_name,
            blob_name=relative_path,
            account_key=self._account_key,
            permission=sas_permissions,
            # the link is valid for "minutes" min.
            expiry=datetime.utcnow() + timedelta(minutes=minutes),
        )
        return sas_token

    # default to 60 min if nothing mentioned.
    def url(self, relative_path):
        """
        Signed url which can be used to directly download the file via redirection.
        """
        blob_path = self.fspath(relative_path=relative_path)

        sas_token = self.generate_sas_token(relative_path)

        return f"{blob_path}?{sas_token}"

    def generate_container_signed_url(self, path, access_time):
        sas_token = self.generate_container_sas_token(
            path,
            access_time,
            sas_permissions=ContainerSasPermissions(read=True, write=True),
        )
        container_url = f"{self._fs_prefix}?{sas_token}"
        return container_url

    def fspath(self, relative_path):
        return os.path.join(self._fs_prefix, relative_path)

    def upload_folder(self, workspace, cloud_base_path):
        """
        This is a helper method to upload an entire folder to cloud.
        workspace - the path to the folder you want to upload.
        cloud_base_path - the base path of the azure blob.
        """
        result = find(workspace)

        prefix = os.path.commonprefix(result)

        for file in result:
            rel_path = os.path.relpath(file, prefix)

            cloud_path = os.path.join(cloud_base_path, rel_path)

            self.upload(file, cloud_path)

            print(f"uploaded {rel_path}")
