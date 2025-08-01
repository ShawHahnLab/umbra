import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="umbra",
    version="0.0.7",
    description="A package and executable for handling Illumina sequencing runs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ShawHahnLab/umbra",
    install_requires=[
        "biopython>=1.79",
        "boxsdk>=3.1.0",
        "pyopenssl", # required for boxsdk but not always pulled in
        "cutadapt>=4.0",
        "pyyaml>=5.1"
        ],
    python_requires='>=3.9,<3.12',
    packages=setuptools.find_packages(exclude=["test_*"]),
    include_package_data=True,
    entry_points={'console_scripts': [
        'umbra=umbra.__main__:main',
    ]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)",
        "Operating System :: POSIX :: Linux",
    ],
)
