#!/usr/bin/env python3
"""
Setup script for F1 Data Pipeline package.
Creates a wheel distribution for AWS Glue deployment.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "F1 Data Engineering Pipeline"

# Read requirements from different files based on environment
def read_requirements(env='glue'):
    """Read requirements from environment-specific files."""
    requirements_files = {
        'glue': 'requirements-glue.txt',
        'mwaa': 'requirements-mwaa.txt', 
        'local': 'requirements.txt'
    }
    
    requirements_path = os.path.join(os.path.dirname(__file__), requirements_files.get(env, 'requirements-glue.txt'))
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    name="f1-pipeline",
    version="1.0.0",
    description="F1 Data Engineering Pipeline for AWS Glue",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="F1 Data Team",
    author_email="data@f1.com",
    url="https://github.com/your-org/f1-data-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=read_requirements('glue'),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "glue": read_requirements('glue'),
        "mwaa": read_requirements('mwaa'),
        "local": read_requirements('local')
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    keywords="f1, data, pipeline, aws, glue, spark, apache-iceberg",
    include_package_data=True,
    zip_safe=False,
)
