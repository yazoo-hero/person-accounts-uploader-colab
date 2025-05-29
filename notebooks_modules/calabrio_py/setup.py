from setuptools import find_packages, setup

setup(
    name="calabrio_py",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "aiohttp>=3.8.0",
        "requests>=2.31.0",
    ],
    python_requires=">=3.8",
)
