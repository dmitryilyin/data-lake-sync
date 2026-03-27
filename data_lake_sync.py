import gzip
import io
import logging
import os
from datetime import datetime

import boto3
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from loguru import logger
from goodconf import GoodConf, Field
from pathlib import Path


class Settings(GoodConf):
    pretend: bool = Field(False, description='Do not actually do anything', env='PRETEND')

    fs_mode: bool = Field(False, description='Use local folders as source and destination', env='FS_MODE')
    source_path: str = Field('fs_source', description='Path to the source directory', env='SOURCE_PATH')
    destination_path: str = Field('fs_destination', description='Path to the destination directory',
                                  env='DESTINATION_PATH')

    adls_account_name: str = Field('unknown', description='The name of the ADLS account', env='ADLS_ACCOUNT_NAME')
    adls_tenant_id: str = Field(None, description='The tenant id of the ADLS account', env='ADLS_TENANT_ID')
    adls_client_id: str = Field(None, description='The client id of the ADLS account', env='ADLS_CLIENT_ID')
    adls_client_secret: str = Field(None, description='The client secret of the ADLS account', env='ADLS_CLIENT_SECRET')

    s3_region_name: str = Field('us-east-1', description='The destination s3 bucket region name', env='S3_REGION_NAME')
    s3_access_key_id: str = Field('us-east-1', description='The destination s3 bucket access key', env='S3_ACCESS_KEY_ID')
    s3_access_key_secret: str = Field('us-east-1', description='The destination s3 bucket access id', env='S3_ACCESS_KEY_SECRET')

    chunk_size: int = Field(8 * 1024 * 1024, description='Read and write streams with this chunk size', env='CHUNK_SIZE')

    @property
    def adls_account_url(self):
        return f"https://{self.adls_account_name}.dfs.core.windows.net"

    def adls_credentials_present(self):
        return self.adls_tenant_id and self.adls_client_id and self.adls_client_secret

    def s3_credentials_present(self):
        return self.s3_access_key_id and self.s3_access_key_secret

    class Meta:
        env_prefix = "DLS_"
        default_files = [".env"]

class DataLakeSync(object):
    def __init__(self, settings):
        self.settings = settings
        self.log = logger.bind(application=self.__class__.__name__)
        self.archive_extensions = ['.gz', '.zip', '.rar', '.bz2']

        self._adls_client = None
        self._s3_client = None

        self.log.info('Init!')

    @property
    def adls_client(self):
        if self._adls_client is not None:
            return self._adls_client

        if self.settings.adls_credentials_present:
            self.log.info("Azure auth: service-principal credential")
            credential = ClientSecretCredential(
                tenant_id=self.settings.adls_tenant_id,
                client_id=self.settings.adls_client_id,
                client_secret=self.settings.adls_client_secret,
            )
        else:
            self.log.info("Azure auth: DefaultAzureCredential (Managed Identity / az login / env)")
            credential = DefaultAzureCredential()

        try:
            self._adls_client = DataLakeServiceClient(account_url=self.settings.adls_account_url, credential=credential)
            return self._adls_client
        except Exception as exception:
            raise RuntimeError(f"Could not connect to the ADLS account! {exception}")

    def s3_client(self):
        if self._s3_client is not None:
            return self._s3_client
        client_settings = {"region_name": self.settings.s3_region_name}
        if self.settings.s3_credentials_present:
            client_settings["aws_access_key_id"] = self.settings.s3_access_key_id
            client_settings["aws_secret_access_key"] = self.settings.s3_access_key_secret
        try:
            self._s3_client = boto3.client("s3", **client_settings)
            return self._s3_client
        except Exception as exception:
            raise RuntimeError(f"Could not connect to the S3 bucket! {exception}")

    def source_iterate_fs(self):
        source_root = Path(self.settings.source_path).resolve()
        self.log.info(
            'Starting source fs iterator for: {source_path} => {root_path}',
            source_path=self.settings.source_path,
            root_path=source_root,
        )
        if not Path.is_dir(source_root):
            raise RuntimeError(f'Source path {source_root} does not exist!')
        for file_path in source_root.rglob('*'):
            if file_path == source_root:
                continue
            if not Path.is_file(file_path):
                continue

            timestamp = file_path.stat().st_mtime
            update_date = datetime.fromtimestamp(timestamp)

            yield file_path, update_date

    def sync_fs(self):
        for source_file_path, source_update_date in self.source_iterate_fs():
            self.log.info(
                'Processing source file: {source_file_path} with update date: {source_update_date}',
                source_file_path=source_file_path,
                source_update_date=source_update_date,
            )
            is_already_archive = source_file_path.suffix.lower() in self.archive_extensions

            destination_root = Path(self.settings.destination_path).resolve()
            if not Path.is_dir(destination_root):
                raise RuntimeError(f"Destination path: {destination_root} does not exist!")

            date_prefix = source_update_date.strftime("%Y-%m-%d")

            source_root = Path(self.settings.source_path).resolve()
            source_file_relative_path = source_file_path.relative_to(source_root)

            destination_file_path = destination_root.joinpath(date_prefix, source_file_relative_path)
            if not is_already_archive:
                destination_file_path = destination_file_path.with_name(destination_file_path.name + '.gz')

            self.log.info(
                'Destination file will be: {destination_file_path} Source is already archive: {is_already_archive}',
                destination_file_path=destination_file_path,
                is_already_archive=is_already_archive,
            )

            destination_file_path.parent.mkdir(parents=True, exist_ok=True)

            if is_already_archive:

                with source_file_path.open('rb') as source_file_in:
                    with destination_file_path.open('wb') as destination_file_out:
                        try:
                            while True:
                                chunk = source_file_in.read(self.settings.chunk_size)
                                if not chunk:
                                    break
                                destination_file_out.write(chunk)
                        except Exception as exception:
                            raise RuntimeError(exception)

            else:

                with source_file_path.open('rb') as source_file_in:
                    with destination_file_path.open('wb') as destination_file_out:
                        destination_gz_out = gzip.GzipFile(fileobj=destination_file_out, mode="wb")
                        try:
                            while True:
                                chunk = source_file_in.read(self.settings.chunk_size)
                                if not chunk:
                                    break
                                destination_gz_out.write(chunk)
                        except Exception as exception:
                            raise RuntimeError(exception)
                        finally:
                            destination_gz_out.close()

            self.log.info(
                'Finished writing from {source_file_path} to {destination_file_path}',
                source_file_path=source_file_path,
                destination_file_path=destination_file_path,
                is_already_archive=is_already_archive,
            )

    def main(self):
        self.log.info('Start!')
        self.sync_fs()


if __name__ == "__main__":
    settings = Settings()
    settings.load()
    dls = DataLakeSync(settings=settings)
    dls.main()
