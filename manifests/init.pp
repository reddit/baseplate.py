# Installation of initial dependencies for devving and testing baseplate.
# Please add additional dependencies and env setup code as needed.
class baseplate {
  exec { 'reddit-repo-add':
    command => '/usr/bin/add-apt-repository ppa:reddit/ppa -y',
    unless  => '/usr/bin/apt-cache policy | /bin/grep reddit',
    notify  => Exec['update-apt'],
  }

  exec { 'update-apt':
    command     => '/usr/bin/apt-get update -q',
    refreshonly => true,
  }

  $packages = [
    'fbthrift-compiler',
    'make',
    'pep8',
    'pylint',
    'python',
    'python3-cassandra',
    'python3-coverage',
    'python3-fbthrift',
    'python3-nose',
    'python3-posix-ipc',
    'python3-pymemcache',
    'python3-redis',
    'python3-requests',
    'python3-setuptools',
    'python3-sqlalchemy',
    'python3-webtest',
    'python-alabaster',
    'python-cassandra',
    'python-coverage',
    'python-dev',
    'python-enum34',
    'python-fbthrift',
    'python-gevent',
    'python-mock',
    'python-nose',
    'python-posix-ipc',
    'python-pymemcache',
    'python-pyramid',
    'python-redis',
    'python-setuptools',
    'python-sphinx',
    'python-sphinxcontrib.spelling',
    'python-sqlalchemy',
    'python-webtest',
  ]

  package { $packages:
    ensure  => installed,
    require => Exec['update-apt'],
  }
}

include baseplate
