import gzip
import io
import sys
import os
from datetime import timezone

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from loguru import logger
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self):
        self.pretend = os.environ.get("PRETEND", "false").lower() == "true"

        self.adls_account_name = os.environ.get("ADLS_ACCOUNT_NAME")
        self.adls_tenant_id = os.environ.get("ADLS_TENANT_ID")
        self.adls_client_id = os.environ.get("ADLS_CLIENT_ID")
        self.adls_client_secret = os.environ.get("ADLS_CLIENT_SECRET")

        self.adls_container_name = os.environ.get("ADLS_CONTAINER_NAME")
        self.adls_path_prefix = os.environ.get("ADLS_PATH_PREFIX", "/")

        self.s3_region_name = os.environ.get("S3_REGION_NAME", "us-east-1")
        self.s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
        self.s3_access_key_id = os.environ.get("S3_ACCESS_KEY_ID")
        self.s3_access_key_secret = os.environ.get("S3_ACCESS_KEY_SECRET")

        self.s3_check_if_exists = os.environ.get("S3_CHECK_IF_EXISTS", "true").lower() == "true"
        self.s3_date_prefix = os.environ.get("S3_DATE_PREFIX", "%Y-%m-%d")
        self.s3_path_separator = os.environ.get("S3_PATH_SEPARATOR", "/")

        self.chunk_size = int(os.environ.get("CHUNK_SIZE", 8 * 1024 * 1024))
        self.log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

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

        self.log.info(
            "Creating the ADLS client for account: {account}",
            account=self.settings.adls_account_name,
        )
        if self.settings.adls_credentials_present:
            credential = ClientSecretCredential(
                tenant_id=self.settings.adls_tenant_id,
                client_id=self.settings.adls_client_id,
                client_secret=self.settings.adls_client_secret,
            )
        else:
            credential = DefaultAzureCredential()

        try:
            self._adls_client = DataLakeServiceClient(
                account_url=self.settings.adls_account_url,
                credential=credential,
                max_chunk_get_size=self.settings.chunk_size,
                max_single_get_size=self.settings.chunk_size,
            )
            return self._adls_client
        except Exception as exception:
            raise RuntimeError(f"Could not connect to the ADLS account! {exception}")

    @property
    def s3_client(self):
        if self._s3_client is not None:
            return self._s3_client
        self.log.info(
            "Creating the S3 client for bucket: {bucket} in: {region}",
            bucket=self.settings.s3_bucket_name,
            region=self.settings.s3_region_name,
        )
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
        self.log.debug("List the ADLS files in: {path}", path=path)
        paths = fs_client.get_paths(path=path, recursive=True)
        for file_path in paths:
            if not file_path.is_directory:
                yield file_path

    def adls_file_date(self, adls_file):
        self.log.debug("Get the last modified date for the ADLS file: {file}", file=adls_file.name)
        date_last_modified = getattr(adls_file, "last_modified", None)
        if date_last_modified is None:
            return None
        if date_last_modified.tzinfo is None:
            date_last_modified = date_last_modified.replace(tzinfo=timezone.utc)
        return date_last_modified

    def adls_is_archive(self, adls_file):
        return Path(adls_file.name).suffix in self.archive_extensions

    def s3_file_path(self, adls_file_path, adls_file_date, file_is_archive):
        adls_file_path_clean = adls_file_path.lstrip("/")
        if len(self.settings.s3_date_prefix) > 0:
            adls_file_date_prefix = adls_file_date.strftime(self.settings.s3_date_prefix)
            s3_file_path = "{0}{1}{2}".format(
                adls_file_date_prefix,
                self.settings.s3_path_separator,
                adls_file_path_clean,
            )
        else:
            s3_file_path = adls_file_path_clean

        if not file_is_archive:
            s3_file_path += '.gz'
        return s3_file_path

    def s3_file_exists(self, file_path):
        try:
            self.s3_client.head_object(Bucket=self.settings.s3_bucket_name, Key=file_path)
            self.log.debug(
                "The file: {path} already exists in S3 bucket: {bucket}",
                path=file_path,
                bucket=self.settings.s3_bucket_name,
            )
            return True
        except ClientError as exception:
            if exception.response['Error']['Code'] != "404":
                raise exception
        self.log.debug(
            "The file: {path} does not exist in S3 bucket: {bucket}",
            path=file_path,
            bucket=self.settings.s3_bucket_name,
        )
        return False

    def sync(self):
        adls_fs_client = self.adls_client.get_file_system_client(file_system=self.settings.adls_container_name)

        files_total = 0
        files_uploaded = 0
        files_skipped = 0
        files_error = 0

        for adls_file in self.adls_files(adls_fs_client, self.settings.adls_path_prefix):
            files_total += 1

            adls_file_path = adls_file.name
            adls_file_date = self.adls_file_date(adls_file)

            if not adls_file_date:
                self.log.warning("Could not get the modification date for file: {file}", file=adls_file_path)
                files_error += 1
                continue

            adls_file_is_archive = self.adls_is_archive(adls_file)
            s3_file_path = self.s3_file_path(adls_file_path, adls_file_date, adls_file_is_archive)

            self.log.info(
                "Syncing ADLS file: {path} [{date}] archive={archive} to the S3 file: {s3_file}",
                path=adls_file_path,
                date=adls_file_date,
                archive=adls_file_is_archive,
                s3_file=s3_file_path,
            )

            if self.settings.pretend:
                self.log.debug("Pretend is enabled, skipping...")
                files_skipped += 1
                continue

            if self.settings.s3_check_if_exists:
                if self.s3_file_exists(s3_file_path):
                    self.log.info("S3 file: {file} already exists, skipping...", file=s3_file_path)
                    files_skipped += 1
                    continue

            adls_file_client = adls_fs_client.get_file_client(adls_file_path)
            adls_file_download = adls_file_client.download_file()

            with io.BytesIO() as upload_buffer:

                try:
                    if adls_file_is_archive:
                        self.log.debug("File is already an archive. Uploading without compression.")
                        for chunk in adls_file_download.chunks():
                            upload_buffer.write(chunk)
                    else:
                        self.log.debug("File is not archive. Compressing the file with gzip during upload.")
                        with gzip.GzipFile(fileobj=upload_buffer, mode='wb') as gz_io:
                            for chunk in adls_file_download.chunks():
                                gz_io.write(chunk)
                except Exception as exception:
                    files_error += 1
                    self.log.warning(
                        "Reading the file: {file} from the ADLS container: {container} failed! {exception}",
                        file=adls_file_path,
                        container=self.settings.adls_container_name,
                        exception=exception
                    )
                    continue

                upload_buffer.seek(0)

                try:
                    self.s3_client.upload_fileobj(
                        upload_buffer,
                        self.settings.s3_bucket_name,
                        s3_file_path,
                        Config=TransferConfig(multipart_chunksize=self.settings.chunk_size),
                    )
                except Exception as exception:
                    files_error += 1
                    self.log.warning(
                        "Upload of file: {file} to the bucket: {bucket} failed! {exception}",
                        file=s3_file_path,
                        bucket=self.settings.s3_bucket_name,
                        exception=exception
                    )
                    continue

            self.log.info(
                "Successfully uploaded file: {adls_path} to the S3 bucket: {s3_bucket} at: {s3_path}",
                adls_path=adls_file_path,
                s3_bucket=self.settings.s3_bucket_name,
                s3_path=s3_file_path,
            )
            files_uploaded += 1

        self.log.info(
            "Sync Stats: total={total}, uploaded={uploaded}, skipped={skipped}, error={error}",
            total=files_total,
            uploaded=files_uploaded,
            skipped=files_skipped,
            error=files_error,
        )

    def main(self):
        self.log.info('Script START')
        self.sync()
        self.log.info('Script END')


if __name__ == "__main__":
    settings = Settings()
    logger.remove()
    logger.add(sys.stdout, level=settings.log_level)
    DataLakeSync(settings=settings).main()
