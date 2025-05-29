"""Setup configuration for person-accounts-uploader package."""
from setuptools import setup, find_packages

# Read the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="person-accounts-uploader",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Validation and upload tool for person accounts data between Workday and Calabrio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/person-accounts-uploader-colab",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Business/Enterprise",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "dash>=2.14.0",
        "pandas>=2.0.0",
        "openpyxl>=3.1.0",
        "python-dotenv>=1.0.0",
        "requests>=2.31.0",
        "plotly>=5.17.0",
        "pydantic>=2.5.0",
        "jupyter-dash>=0.4.2",
        "typing-extensions>=4.8.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.11.0",
            "black>=23.7.0",
            "isort>=5.12.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
            "pylint>=2.17.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "person-accounts-uploader=src.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.json", "*.yaml", "*.yml"],
    },
)