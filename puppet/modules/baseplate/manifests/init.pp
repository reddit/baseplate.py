# packages and tools needed for a baseplate development environment
class baseplate {
  exec { 'add reddit ppa':
    command => 'add-apt-repository -y ppa:reddit/ppa',
    unless  => 'apt-cache policy | grep reddit/ppa',
    notify  => Exec['update apt cache'],
  }

  $packages = [
    # utilities
    'python',
    'python3',
    'fbthrift-compiler',
    'make',
    'pylint',
    'python-flake8',

    # libraries
    'python3-cassandra',
    'python3-coverage',
    'python3-cqlmapper',
    'python3-fbthrift',
    'python3-gevent',
    'python3-hvac',
    'python3-nose',
    'python3-posix-ipc',
    'python3-pymemcache',
    # TODO: this package currently conflicts with the py2 version
    # because they both have binaries with the same name.
    # 'python3-pyramid',
    'python3-redis',
    'python3-requests',
    'python3-setuptools',
    'python3-sqlalchemy',
    'python3-webtest',
    'python-coverage',
    'python-cqlmapper',
    'python-fbthrift',
    'python-gevent',
    'python-hvac',
    'python-nose',
    'python-posix-ipc',
    'python-pymemcache',
    'python-pyramid',
    'python-redis',
    'python-requests',
    'python-setuptools',
    'python-sqlalchemy',
    'python-webtest',

    # sphinx
    'python-alabaster',
    'python-sphinx',
    'python-sphinxcontrib.spelling',

    # compatibility
    'python-enum34',
    'python-mock',

    # debian packaging
    'devscripts',
    'debhelper',
    'python-all',
    'python3-all',
  ]

  package { $packages:
    ensure => installed,
  }
}
