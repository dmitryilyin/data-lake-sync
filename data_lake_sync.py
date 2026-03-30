import gzip
import io
import logging
import os
from datetime import datetime
from datetime import timezone

import boto3
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from loguru import logger

from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self):
        self.pretend = os.environ.get("PRETEND", "false").lower() == "true"

        self.fs_mode = os.environ.get("FS_MODE", "false").lower() == "true"
        self.source_path = os.environ.get("SOURCE_PATH", "fs_source")
        self.destination_path = os.environ.get("DESTINATION_PATH", "fs_destination")

        self.adls_account_name = os.environ.get("ADLS_ACCOUNT_NAME")
        self.adls_tenant_id = os.environ.get("ADLS_TENANT_ID")
        self.adls_client_id = os.environ.get("ADLS_CLIENT_ID")
        self.adls_client_secret = os.environ.get("ADLS_CLIENT_SECRET")

        self.adls_container_name = os.environ.get("ADLS_CONTAINER_NAME")
        self.adls_path_prefix = os.environ.get("ADLS_PATH_PREFIX", "/")

        self.s3_region_name = os.environ.get("S3_REGION_NAME", "us-east-1")
        self.s3_access_key_id = os.environ.get("S3_ACCESS_KEY_ID")
        self.s3_access_key_secret = os.environ.get("S3_ACCESS_KEY_SECRET")

        self.chunk_size = int(os.environ.get("CHUNK_SIZE", 8 * 1024 * 1024))

    @property
    def adls_account_url(self):
        if not self.adls_account_name:
            return None
        return f"https://{self.adls_account_name}.dfs.core.windows.net"

    @property
    def adls_credentials_present(self):
        return bool(self.adls_tenant_id and self.adls_client_id and self.adls_client_secret)

    @property
    def s3_credentials_present(self):
        return bool(self.s3_access_key_id and self.s3_access_key_secret)


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

        self.log.info("Creating the ADLS client for account: {account}", account=self.settings.adls_account_name)
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

    def adls_files(self, fs_client, path="/"):
        self.log.info("List ADLS files inside path: {path}", path=path)
        paths = fs_client.get_paths(path=path, recursive=True)
        for file_path in paths:
            if not file_path.is_directory:
                yield file_path

    def adls_file_date(self, adls_file):
        self.log.info("Get last modified date for the ADLS file: {file}", file=adls_file.name)
        date_last_modified = getattr(adls_file, "last_modified", None)
        if date_last_modified is None:
            return None
        if date_last_modified.tzinfo is None:
            date_last_modified = date_last_modified.replace(tzinfo=timezone.utc)
        return date_last_modified

    def sync(self):
        fs_client = self.adls_client.get_file_system_client(file_system=self.settings.adls_container_name)
        for adls_file in self.adls_files(fs_client, self.settings.adls_path_prefix):
            adls_file_path = adls_file.name
            adls_file_date = self.adls_file_date(adls_file)
            file_is_archive = adls_file_path.lower().endswith(".gz")
            self.log.info(
                "ADLS file: {path} [{date}] Archive: {archive}",
                path=adls_file_path,
                date=adls_file_date,
                archive=file_is_archive,
            )

    # def source_iterate_fs(self):
    #     source_root = Path(self.settings.source_path).resolve()
    #     self.log.info(
    #         'Starting source fs iterator for: {source_path} => {root_path}',
    #         source_path=self.settings.source_path,
    #         root_path=source_root,
    #     )
    #     if not Path.is_dir(source_root):
    #         raise RuntimeError(f'Source path {source_root} does not exist!')
    #     for file_path in source_root.rglob('*'):
    #         if file_path == source_root:
    #             continue
    #         if not Path.is_file(file_path):
    #             continue
    #
    #         timestamp = file_path.stat().st_mtime
    #         update_date = datetime.fromtimestamp(timestamp)
    #
    #         yield file_path, update_date
    #
    # def sync_fs(self):
    #     for source_file_path, source_update_date in self.source_iterate_fs():
    #         self.log.info(
    #             'Processing source file: {source_file_path} with update date: {source_update_date}',
    #             source_file_path=source_file_path,
    #             source_update_date=source_update_date,
    #         )
    #         is_already_archive = source_file_path.suffix.lower() in self.archive_extensions
    #
    #         destination_root = Path(self.settings.destination_path).resolve()
    #         if not Path.is_dir(destination_root):
    #             raise RuntimeError(f"Destination path: {destination_root} does not exist!")
    #
    #         date_prefix = source_update_date.strftime("%Y-%m-%d")
    #
    #         source_root = Path(self.settings.source_path).resolve()
    #         source_file_relative_path = source_file_path.relative_to(source_root)
    #
    #         destination_file_path = destination_root.joinpath(date_prefix, source_file_relative_path)
    #         if not is_already_archive:
    #             destination_file_path = destination_file_path.with_name(destination_file_path.name + '.gz')
    #
    #         self.log.info(
    #             'Destination file will be: {destination_file_path} Source is already archive: {is_already_archive}',
    #             destination_file_path=destination_file_path,
    #             is_already_archive=is_already_archive,
    #         )
    #
    #         destination_file_path.parent.mkdir(parents=True, exist_ok=True)
    #
    #         if is_already_archive:
    #
    #             with source_file_path.open('rb') as source_file_in:
    #                 with destination_file_path.open('wb') as destination_file_out:
    #                     try:
    #                         while True:
    #                             chunk = source_file_in.read(self.settings.chunk_size)
    #                             if not chunk:
    #                                 break
    #                             destination_file_out.write(chunk)
    #                     except Exception as exception:
    #                         raise RuntimeError(exception)
    #
    #         else:
    #
    #             with source_file_path.open('rb') as source_file_in:
    #                 with destination_file_path.open('wb') as destination_file_out:
    #                     destination_gz_out = gzip.GzipFile(fileobj=destination_file_out, mode="wb")
    #                     try:
    #                         while True:
    #                             chunk = source_file_in.read(self.settings.chunk_size)
    #                             if not chunk:
    #                                 break
    #                             destination_gz_out.write(chunk)
    #                     except Exception as exception:
    #                         raise RuntimeError(exception)
    #                     finally:
    #                         destination_gz_out.close()
    #
    #         self.log.info(
    #             'Finished writing from {source_file_path} to {destination_file_path}',
    #             source_file_path=source_file_path,
    #             destination_file_path=destination_file_path,
    #             is_already_archive=is_already_archive,
    #         )

    def main(self):
        self.log.info('Start!')
        self.sync()


if __name__ == "__main__":
    settings = Settings()

    dls = DataLakeSync(settings=settings)
    dls.main()
