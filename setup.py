from distutils.core import setup

setup(name='expdb',
      version='0.01a',
      description='ExpDB',
      author='Vaishaal Shankar',
      author_email='vaishaal@gmail.com',
      packages=['expdb'],
      entry_points={
        "console_scripts": [
            "expdb = expdb.expdb_cli:cli",
        ]}
     )