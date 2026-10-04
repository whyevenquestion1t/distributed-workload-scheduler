"""Setup script for shared module installation."""

from setuptools import setup, find_packages

setup(
    name="distributed-workload-scheduler-shared",
    version="1.0.0",
    description="Shared models used by the scheduler and executor services",
    packages=find_packages(),
    install_requires=[
        "pydantic>=2.0.0",
    ],
    python_requires=">=3.10",
)
