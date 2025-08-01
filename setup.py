from setuptools import setup, find_packages

setup(
    name="ml_data_loader",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["pandas>=1.3", "pyyaml>=6.0"],
    python_requires='>=3.7',
)
