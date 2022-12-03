from boto3.s3.transfer import TransferConfig
from s3transfer.download import DownloadSubmissionTask
from s3transfer.futures import TransferCoordinator, TransferFuture, TransferMeta
from s3transfer.manager import TransferManager
from s3transfer.utils import CallArgs


class MyTransferManager(TransferManager):
    def download_sized(
        self, bucket, key, fileobj, size, extra_args=None, subscribers=None
    ):
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        self._validate_all_known_args(extra_args, self.ALLOWED_DOWNLOAD_ARGS)
        self._validate_if_bucket_supported(bucket)
        call_args = CallArgs(
            bucket=bucket,
            key=key,
            fileobj=fileobj,
            size=size,
            extra_args=extra_args,
            subscribers=subscribers,
        )
        extra_main_kwargs = {"io_executor": self._io_executor}
        return self._submit_transfer(
            call_args, DownloadSubmissionTask, extra_main_kwargs
        )

    def _get_future_with_components(self, call_args):
        transfer_id = self._id_counter
        # Creates a new transfer future along with its components
        transfer_coordinator = TransferCoordinator(transfer_id=transfer_id)
        # Track the transfer coordinator for transfers to manage.
        self._coordinator_controller.add_transfer_coordinator(transfer_coordinator)
        # Also make sure that the transfer coordinator is removed once
        # the transfer completes so it does not stick around in memory.
        transfer_coordinator.add_done_callback(
            self._coordinator_controller.remove_transfer_coordinator,
            transfer_coordinator,
        )
        components = {
            "meta": TransferMeta(call_args, transfer_id=transfer_id),
            "coordinator": transfer_coordinator,
        }
        components["meta"].provide_transfer_size(call_args.size)
        transfer_future = TransferFuture(**components)
        return transfer_future, components


def download_fileobj(self, bucket, key, fileobj, size=None, Config=None):
    config = TransferConfig() if Config is None else Config
    with MyTransferManager(self, config) as manager:
        future = manager.download_sized(
            bucket=bucket,
            key=key,
            fileobj=fileobj,
            size=size,
        )
        return future.result()
