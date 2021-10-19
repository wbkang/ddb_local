import pytest
import tempfile
import os
import shutil

@pytest.fixture
def clean_dir():
    temp = os.path.join(tempfile.mkdtemp(), "ddb-local")
    yield temp
    shutil.rmtree(os.path.dirname(temp))

@pytest.fixture(scope="session")
def default_test_dir():
    temp = os.path.join(tempfile.mkdtemp(), "ddb-local")
    yield temp
    shutil.rmtree(os.path.dirname(temp))

@pytest.fixture
def existing_file():
    (fd, tf) = tempfile.mkstemp()
    yield tf
    os.unlink(tf)

@pytest.fixture
def java_home():
    java_bin = shutil.which("java")
    java_home = os.path.join(os.readlink(java_bin), "..")
    os.environ['JAVA_HOME'] = java_home
    yield
    os.environ.pop('JAVA_HOME')