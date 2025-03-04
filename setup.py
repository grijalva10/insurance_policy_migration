from setuptools import setup, find_packages

setup(
    name="insurance_policy_migration",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "aiohttp",
        "requests",
        "python-dateutil"
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "migrate-policies=insurance_migration.__main__:main"
        ]
    }
) 