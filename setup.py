import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ceph_report",
    version="3.0.0",
    author="Kostiantyn Danylov aka koder",
    author_email="koder.mail@gmail.com",
    description="Ceph cluster information collector & report generator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Mirantis/ceph-monitoring",
    packages=setuptools.find_packages(),
    python_requires=">=3.7.2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)