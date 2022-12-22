from setuptools import setup

setup(
    name='ddb_local',
    version='0.1.6',
    packages=['ddb_local',],
    license='MIT',
    author="Woongbin Kang",
    author_email="pypi@wbk.one",
    url="https://github.com/wbkang/ddb_local",
    long_description=open("README.md").read(),
    long_description_content_type="text/plain",
    install_requires=["requests"],
)