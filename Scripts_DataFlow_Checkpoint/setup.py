from setuptools import setup, find_packages

requires=['pymysql','psycopg2','apache-beam[gcp]','gcsfs','workflow','google-cloud-storage']

setup(
    name="dataflow_pipeline_dependencies",
    url="www.gentera.com",
    version="1.0.0",
    author='Luis Miguel Salazar E',
    author_email='e-rmontano@externos.gentera.com.mx',
    maintainer='Juan Victor Pita Cangalaya',
    maintainer_email='e-jpita@externos.gentera.com.mx',
    description="Beam pipeline for flattening ism data",
    data_files=[(".", ["GEO/client-cert.pem","GEO/client-key.pem","GEO/server-ca.pem",])],
    packages=find_packages(),
    install_requires=requires,
    include_package_data=True
)