import tempfile
import os
import logging
from typing import List, Optional
import requests
import tarfile
import subprocess
import time
import socket
import shutil

logger = logging.getLogger("ddb_local")

DEFAULT_DOWNLOAD_URL = (
    "https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"
)
DEFAULT_UNPACK_DIR = os.path.join(tempfile.gettempdir(), "ddb_local")
DEFAULT_CHUNK_SIZE = 256 * 1024
DEFAULT_REACHABLE_TIMEOUT = 3
DEFAULT_KILL_WAIT_TIME = 5


def is_within_directory(directory, target):
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)

    prefix = os.path.commonprefix([abs_directory, abs_target])

    return prefix == abs_directory


class LocalDynamoDB(object):
    """
    DynamoDBLocal wrapper for Python.

    Use it in a context manager. Example:
    with LocalDynamoDB() as ddb:
        print(f"Endpoint is {ddb.endpoint}")
    """

    def __init__(
        self,
        source_url=DEFAULT_DOWNLOAD_URL,
        unpack_dir=DEFAULT_UNPACK_DIR,
        debug=False,
        port=8000,
        in_memory=False,
        db_path=None,
        shared_db=False,
        extra_args=[],
    ):
        """
        Makes a wrapper for DynamoDBLocal.

        Args:
            source_url: Where to download the tarball from. Defaults to Oregon.
            Find the full list from https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
            unpack_dir: Where to install local dynamodb. It has to be an empty directory.
            debug: If true, will direct stdout/stderr from DynamoDBLocal.
            port: Port for the DynamoDBLocal.
            in_memory: Set to True to make the database to be in memory. The content will be lost after shutdown.
            db_path: The directory where DynamoDB will save its database. Defaults to unpack_dir. Mutually exclusive with in_memory.
            shared_db: Share DB across all credentials.
            extra_args: Arguments to forward to DynamoDBLocal.
        """
        self.source_url: str = DEFAULT_DOWNLOAD_URL
        self.unpack_dir: str = unpack_dir
        self.debug: bool = debug
        self.port: int = port
        self.in_memory = in_memory
        self.extra_args: List[str] = extra_args
        self.java_bin: Optional[str] = None
        self.ddb_process: Optional[subprocess.Popen] = None
        self.endpoint: str = f"http://localhost:{port}"
        self.shared_db: bool = shared_db
        self.db_path: Optional[str] = None
        if db_path:
            self.db_path = os.path.abspath(db_path)
            os.makedirs(self.db_path, exist_ok=True)
        if in_memory and db_path is not None:
            raise Exception("Can't be both in_memory and on-disk")

    def _ensure_installed(self):
        if os.path.exists(self.unpack_dir):
            if not os.path.isdir(self.unpack_dir):
                raise Exception(
                    f"Target unpack_dir[{self.unpack_dir}] exists but is not a directory! Delete it or change your unpack_dir."
                )
            else:
                logger.debug(f"unpack_dir[{self.unpack_dir}] already exists")
                return
        else:
            os.makedirs(self.unpack_dir, exist_ok=True)
            with requests.get(self.source_url, stream=True) as req:
                req.raise_for_status()

                cur_cwd = os.getcwd()
                # tarfile.extract works relative to the tar file.
                os.chdir(self.unpack_dir)
                success = False
                try:
                    with tarfile.open(fileobj=req.raw, mode="r:gz") as tf:
                        while True:
                            member = tf.next()
                            if not member:
                                break
                            member_path = os.path.join(self.unpack_dir, member.name)
                            if not is_within_directory(self.unpack_dir, member_path):
                                raise Exception(
                                    f"Attempted Path Traversal in Tar File. Path:{member_path}"
                                )
                            tf.extract(member)
                    success = True
                finally:
                    # delete if the extraction failed.
                    if not success:
                        shutil.rmtree(self.unpack_dir, ignore_errors=True)
                    os.chdir(cur_cwd)

    def _ensure_port_free(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("localhost", self.port))
        except:
            raise Exception(f"Port {self.port} is not free.")

    def _ensure_java_exists(self):
        java_home = os.environ.get("JAVA_HOME")
        if java_home is not None:
            try:
                java_bin = os.path.join(java_home, "bin", "java")
                subprocess.check_output([java_bin, "-version"])
                self.java_bin = java_bin
                logger.info(f"Using java at [{java_bin}]")
                return
            except:
                logger.warning(
                    f"JAVA_HOME is specified [{java_home}] but java -version failed to run. Trying java next."
                )
        try:
            subprocess.check_output(["java", "-version"])
            self.java_bin = "java"
            logger.info(f"Using java in PATH")
        except:
            raise Exception(
                "Failed to execute java. Either specify JAVA_HOME or have Java in PATH"
            )

    def _ensure_reachable(self):
        wait_until = time.time() + DEFAULT_REACHABLE_TIMEOUT
        while time.time() <= wait_until:
            try:
                requests.get(self.endpoint)
                return
            except:
                logger.debug(f"Can't reach DDB at {self.endpoint}")
        raise Exception("DynamoDB never became reachable.")

    def _start_ddb_local(self):
        stdout = subprocess.DEVNULL
        stderr = subprocess.DEVNULL
        if self.debug:
            stdout = None
            stderr = None

        args = [
            self.java_bin,
            "-Djava.library.path=DynamoDBLocal_lib",
            "-jar",
            "DynamoDBLocal.jar",
            "-port",
            str(self.port),
        ]
        if self.in_memory:
            args.append("-inMemory")
        if self.db_path is not None:
            args.append("-dbPath")
            args.append(self.db_path)

        if self.shared_db:
            args.append("-sharedDb")
        args.extend(self.extra_args)
        self.ddb_process = subprocess.Popen(
            args, cwd=self.unpack_dir, stdout=stdout, stderr=stderr
        )
        if self.ddb_process.returncode is not None:
            raise Exception(
                f"DynamoDBLocal failed to start with exit code [{self.ddb_process.returncode}]"
            )

    def _shutdown_ddb_local(self):
        if self.ddb_process:
            self.ddb_process.terminate()
            try:
                self.ddb_process.wait(timeout=DEFAULT_KILL_WAIT_TIME)
                self.ddb_process = None
                logger.debug("DynamoDB successfully shutdown")
            except subprocess.TimeoutExpired:
                logger.warn(
                    f"DynamoDB failed to shutdown in {DEFAULT_KILL_WAIT_TIME} seconds. Killing it. PID={self.ddb_process.pid}"
                )
                self.ddb_process.kill()
                self.ddb_process = None

    def start(self):
        self._ensure_port_free()
        self._ensure_java_exists()
        self._ensure_installed()
        self._start_ddb_local()
        self._ensure_reachable()
        logger.debug(f"LocalDynamoDB starting with {vars(self)}")

    def stop(self):
        self._shutdown_ddb_local()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


def create_new_inmemory_ddb() -> LocalDynamoDB:
    """
    Creates a throwaway in-memory only ddb on a random port.
    Check the object's endpoint attribute to find out where to connect to.

    Finding a free port is on a best-effort basis -
    There is a chance of race condition with respect to the port finding.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return LocalDynamoDB(port=port, in_memory=True)
