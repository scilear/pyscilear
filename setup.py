from setuptools import setup

setup(name='pyscilear',
      version='2018.07.24',
      description='scilear utilities',
      url='https://github.com/scilear/pyscilear.git',
      author='Scilear',
      author_email='scilear@gmail.com',
      license='MIT',
      packages=['pyscilear'],
      zip_safe=False, requires=['logbook', 'psycopg2', 'sqlalchemy', 'pandas', 'boto'])
