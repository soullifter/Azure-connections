import datetime
import os
import sys
import time
from typing import List

import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from utils import find

DEFAULT_ENCODING = "utf-8"


def wrap_commands_in_shell(commands: List[str]) -> str:
    """Wrap commands in a shell

    This helps in wrapping list of commands to run.
    """
    return f"/bin/bash -c " f'\'set -e; set -o pipefail; {";".join(commands)}; wait\''


def create_pool_if_not_exist(
    batch_client: BatchServiceClient, pool: batchmodels.PoolAddParameter
):
    """Creates the specified pool if it doesn't already exist

    :param batch_client: The batch client to use.
    :param pool: The pool to create.
    """
    try:
        print("Attempting to create pool:", pool.id)
        batch_client.pool.add(pool)
        print("Created pool:", pool.id)
    except batchmodels.BatchErrorException as err:
        if err.error.code != "PoolExists":
            raise
        else:
            print(f"Pool {pool.id!r} already exists")


class BatchClient:
    """
    With the help of BatchClient class we can interact with azure
    batch and submit jobs based on downstream requirement.

    params: config,
    it should contain the following information about batch

    name : "{}"  # Your batch account name
    url : "https://{}.eastus.batch.azure.com"  # Your batch account url
    key : ""  # Your batch account key
    pool:
        name: "{}"  # Your Pool ID
        sku: "standard_d2s_v3"
        vm_count: 8
    stdout: "stdout.txt"

    params: storage, Storage client to interact with azure blob storage.
    """

    def __init__(self, config, storage):
        self._config = config
        self._storage = storage
        credentials = SharedKeyCredentials(config["name"], config["key"])
        self._batch_service_client = BatchServiceClient(
            credentials, batch_url=config["url"]
        )

    @property
    def config(self):
        return self._config

    @property
    def storage(self):
        return self._storage

    @property
    def batch_service_client(self):
        return self._batch_service_client

    def create_pool_and_wait_for_node(
        self,
        pool_id: str,
        vm_size: str,
        vm_count: int,
    ):
        """Creates an Azure Batch pool with the specified id.

        :param pool_id: The id of the pool to create.
        :param vm_size: vm size (sku)
        :param vm_count: number of vms to allocate
        """
        # create start task commands
        # 1. update repository
        # 2. install blobxfer pre-requisites
        # 3. pip install blobxfer python script
        start_task_commands = [
            "apt-get update",
            "apt-get install -y build-essential libssl-dev libffi-dev "
            + "libpython3-dev python3-dev python3-pip",
            "python3 -m pip install --upgrade pip",
            "python3 -m pip install --upgrade blobxfer",
            "apt-get -y install docker.io",
            "systemctl enable docker --now",
        ]

        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin,
        )
        # create pool with start task and cert ref with visibility of task
        pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="canonical",
                    offer="0001-com-ubuntu-server-focal",
                    sku="20_04-lts",
                ),
                node_agent_sku_id="batch.node.ubuntu 20.04",
            ),
            vm_size=vm_size,
            target_low_priority_nodes=vm_count,
            start_task=batchmodels.StartTask(
                command_line=wrap_commands_in_shell(start_task_commands),
                user_identity=batchmodels.UserIdentity(auto_user=user),
                wait_for_success=True,
            ),
        )
        create_pool_if_not_exist(self._batch_service_client, pool)

    def create_job(self, job_id: str, pool_id: str):
        """
        Creates a job with the specified ID, associated with the specified pool.

        :param batch_service_client: A Batch service client.
        :param str job_id: The ID for the job.
        :param str pool_id: The ID for the pool.
        """
        print(f"Creating job [{job_id}]...")

        job = batchmodels.JobAddParameter(
            id=job_id,
            pool_info=batchmodels.PoolInformation(pool_id=pool_id),
            on_all_tasks_complete="terminateJob",
        )

        self._batch_service_client.job.add(job)

    def add_resource(self, local_path: str, cloud_path: str):
        """
        First adds the file to azure storage, then get the resource to batch VM.
        """
        self._storage.upload(local_path, cloud_path)
        expiry_in_minutes = 60 * 24
        return self.get_resource(cloud_path, expiry_in_minutes=expiry_in_minutes)

    def get_resource(self, cloud_path, file_path=None, expiry_in_minutes=60 * 24):
        """
        Get the resource to the batch VM from azure storage.
        """
        sas_url = self._storage.url(cloud_path, expiry_in_minutes)
        return batchmodels.ResourceFile(
            http_url=sas_url,
            file_path=(file_path if file_path else cloud_path),
        )

    def wait_for_tasks_to_complete(self, job_id: str, timeout: datetime.timedelta):
        """
        Returns when all tasks in the specified job reach the Completed state.

        :param batch_service_client: A Batch service client.
        :param job_id: The id of the job whose tasks should be to monitored.
        :param timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print(
            f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...",
            end="",
        )

        while datetime.datetime.now() < timeout_expiration:
            print(".", end="")
            sys.stdout.flush()
            tasks = self._batch_service_client.task.list(job_id)

            incomplete_tasks = [
                task for task in tasks if task.state != batchmodels.TaskState.completed
            ]
            if not incomplete_tasks:
                print()
                return True

            time.sleep(1)

        print()
        raise RuntimeError(
            "ERROR: Tasks did not reach 'Completed' state within "
            "timeout period of " + str(timeout)
        )

    def print_batch_exception(self, batch_exception: batchmodels.BatchErrorException):
        """
        Prints the contents of the specified Batch exception.

        :param batch_exception:
        """
        print("-------------------------------------------")
        print("Exception encountered:")
        if (
            batch_exception.error
            and batch_exception.error.message
            and batch_exception.error.message.value
        ):
            print(batch_exception.error.message.value)
            if batch_exception.error.values:
                print()
                for mesg in batch_exception.error.values:
                    print(f"{mesg.key}:\t{mesg.value}")
        print("-------------------------------------------")


"""
Lets see an example how to use this batch client to submit a job to azure batch.
"""


class BatchCheck:
    def __init__(self, batch_client: BatchClient):
        self._batch_client = batch_client

    def add_tasks(self, batch_job_id, resource_files):
        """
        This will add tasks to the batch and run them.
        """

        # Sample commands to check.
        commands = [
            "python3 -m pip install numpy",
            "docker -v",
        ]

        command = wrap_commands_in_shell(commands)

        admin_user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin,
        )

        tasks = []

        tasks.append(
            batchmodels.TaskAddParameter(
                id="check",
                command_line=command,
                resource_files=resource_files,
                user_identity=batchmodels.UserIdentity(auto_user=admin_user),
            )
        )

        self._batch_client.batch_service_client.task.add_collection(batch_job_id, tasks)

    def build(self, workspace, cloud_base_path):
        """
        Workspce is path to folder, we upload the folder to azure storage 
        and get the resources to the azure batch.
        """
        self._batch_client.storage.upload_folder(workspace, cloud_base_path)

        result = find(workspace)

        common_prefix = os.path.commonprefix(result)

        resource_file_paths = [os.path.relpath(file, common_prefix) for file in result]

        resource_files = [
            self._batch_client.get_resource(
                os.path.join(cloud_base_path, resource_file_path),
            )
            for resource_file_path in resource_file_paths
        ]

        batch_job_id = f"checker"

        try:
            pool = self._batch_client.config["pool"]
            self._batch_client.create_pool_and_wait_for_node(
                pool["name"], pool["sku"], pool["vm_count"]
            )
            # Create the job that will run the tasks.
            self._batch_client.create_job(batch_job_id, pool["name"])

            # Add the tasks to the job.
            self.add_tasks(
                batch_job_id,
                resource_files,
                cloud_base_path,
            )

        except batchmodels.BatchErrorException as err:
            self._batch_client.print_batch_exception(err)
            raise
        pass
