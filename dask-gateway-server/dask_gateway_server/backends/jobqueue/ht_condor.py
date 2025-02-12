import math
import os
import shutil
import re
from traitlets import Unicode, default

from ...traitlets import Type
from .base import JobQueueBackend, JobQueueClusterConfig
import asyncio
import json
__all__ = ("HTCondorBackend", "HTCondorClusterConfig")


def htc_format_memory(n):
    """Format memory in bytes for use with slurm."""
    if n >= 10 * (1024**3):
        return "%dGB" % math.ceil(n / (1024**3))
    if n >= 10 * (1024**2):
        return "%dMB" % math.ceil(n / (1024**2))
    if n >= 10 * 1024:
        return "%dK" % math.ceil(n / 1024)
    return "1K"


class HTCondorClusterConfig(JobQueueClusterConfig):
    """Dask cluster configuration options when running on SLURM"""

    partition = Unicode("", help="The partition to submit jobs to.", config=True)

    qos = Unicode("", help="QOS string associated with each job.", config=True)

    account = Unicode("", help="Account string associated with each job.", config=True)


class HTCondorBackend(JobQueueBackend):
    """A backend for deploying Dask on a Slurm cluster."""

    cluster_config_class = Type(
        "dask_gateway_server.backends.jobqueue.slurm.SlurmClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("condor_submit") or "condor_submit"

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("condor_rm") or "condor_rm"

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("condor_q") or "condor_q"

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        # Format HTCondor submit file contents
        submit_contents = [

        ]

        if worker:
            job_name = f"dask-worker-{cluster.id}.{worker.id}"
            cpus = cluster.config.worker_cores
            mem = htc_format_memory(cluster.config.worker_memory)
            log_file = "dask-worker-%s.log" % worker.name
            script = "\n".join(
                [
                    "#!/bin/sh",
                    cluster.config.worker_setup,
                    " ".join(self.get_worker_command(cluster, worker.name)),
                ]
            )
            env = self.get_worker_env(cluster)
        else:
            job_name = f"dask-scheduler-{cluster.id}"
            cpus = cluster.config.scheduler_cores
            mem = htc_format_memory(cluster.config.scheduler_memory)
            log_file = "dask-scheduler-%s.log" % cluster.name
            script = "\n".join(
                [
                    "#!/bin/sh",
                    cluster.config.scheduler_setup,
                    " ".join(self.get_scheduler_command(cluster)),
                ]
            )
            env = self.get_scheduler_env(cluster)


        # Add required configurations
        staging_dir = self.get_staging_directory(cluster)

        submit_contents.extend([
            f"JobBatchName = {job_name}",
            f"output = {os.path.join(staging_dir, log_file)}",
            f"error = {os.path.join(staging_dir, log_file)}.err",
            f"log = {os.path.join(staging_dir, log_file)}.condor",
            f"request_cpus = {cpus}",
            f"request_memory = {mem}",
        ])

        # Get environment variables
        if env:
            env_settings = ' '.join(f"{k}={v}" for k, v in env.items())
            submit_contents.append(f"environment = \"{env_settings}\"")

        script_path = os.path.join(staging_dir, "script.sh")
        submit_contents.append(f"executable = {script_path}")
        # Create the final command
        submit_str = "\n".join(submit_contents)
        cmd = f"echo '{submit_str}' | {self.submit_command} -"

        files = {
            "script.sh": script,
        }

        self.log.info("Submitting job with command: %s", submit_str)
        return self.submit_command, env, submit_str, script

    def get_stop_cmd_env(self, job_id):
        self.log.info("Cancelling job: %s", job_id)
        return [self.cancel_command, job_id], {}

    def get_status_cmd_env(self, job_ids):
        self.log.info("Checking status of jobs: %s", job_ids)
        cmd = [self.status_command, "-json", "-format", "'%d, '", "ClusterId", "-format", "'%s\n'", "JobStatus", " ".join(job_ids)]
        return cmd, {}

    def parse_job_states(self, stdout):
        states = {}
        statuses = json.loads(stdout)
        for status in statuses:
            job_id = status["ClusterId"]
            state = status["JobStatus"]
            self.log.info("Job %s is in state %s", job_id, state)
            states[job_id] = state in ("1", "2", "4", "5", "7")
        return states

    def parse_job_id(self, stdout):
        self.log.info("Parsing job id from stdout: %s", stdout)
        cluster_id = re.findall(r'cluster (\d+)', stdout)[0]
        return cluster_id


    async def do_start_cluster(self, cluster):
        cmd, env, stdin, script = self.get_submit_cmd_env_stdin(cluster)
        staging_dir = self.get_staging_directory(cluster)
        files = {
            "dask.pem": cluster.tls_key.decode("utf8"),
            "dask.crt": cluster.tls_cert.decode("utf8"),
            "script.sh": script,
        }
        job_id = await self.start_job(
            cluster.username, cmd, env, stdin, staging_dir=staging_dir, files=files
        )
        self.log.info("Job %s submitted for cluster %s", job_id, cluster.name)
        yield {"job_id": job_id, "staging_dir": staging_dir}

    async def do_start_worker(self, worker):
        self.log.info(f"Starting worker {worker.name} from cluster job {worker.cluster.state}")
        staging_dir = self.get_staging_directory(worker.cluster)

        cmd, env, stdin, script = self.get_submit_cmd_env_stdin(worker.cluster, worker)
        files = {
            "dask.pem": worker.cluster.tls_key.decode("utf8"),
            "dask.crt": worker.cluster.tls_cert.decode("utf8"),
            "script.sh": script,
        }

        job_id = await self.start_job(
            worker.cluster.username, cmd, env, stdin, staging_dir=staging_dir, files=files
        )
        self.log.info("Job %s submitted for worker %s", job_id, worker.name)
        yield {"job_id": job_id}

    async def do_as_user(self, user, action, **kwargs):
        cmd = [self.dask_gateway_jobqueue_launcher]
        kwargs["action"] = action
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env={},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(json.dumps(kwargs).encode("utf8"))
        stdout = stdout.decode("utf8", "replace")
        stderr = stderr.decode("utf8", "replace")

        if proc.returncode != 0:
            raise Exception(
                "Error running `dask-gateway-jobqueue-launcher`\n"
                "  returncode: %d\n"
                "  stdout: %s\n"
                "  stderr: %s" % (proc.returncode, stdout, stderr)
            )
        result = json.loads(stdout)
        if not result["ok"]:
            raise Exception(result["error"])
        return result["returncode"], result["stdout"], result["stderr"]


    def get_staging_directory(self, cluster):
        self.log.info(f"o-------> {cluster.config.staging_directory}")
        return os.path.join(cluster.config.staging_directory, cluster.name)
