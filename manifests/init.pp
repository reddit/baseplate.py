# Installation of initial dependencies and baseplate project
# in a development environment.
# Please add additional dependencies and env setup code as needed.
class baseplate {
  $project_path = '/home/vagrant/baseplate'

  Exec { path => [ '/usr/bin', '/usr/sbin', '/bin', '/usr/local/bin' ] }

  # Add reddit package apt-repo
  exec { 'reddit-repo-add':
    command => 'sudo add-apt-repository ppa:reddit/ppa -y',
    unless  => 'apt-cache policy | grep reddit',
    notify  => Exec['update-apt'],
  }

  # Update apt
  exec { 'update-apt':
    command     => 'sudo apt-get update -q',
    refreshonly => true,
  }

  # Install the dependencies
  package {
    [
      'python',
      'python3-setuptools',
      'python-setuptools',
      'python-dev',
      'python-pip',
      'python-gevent',
      'python-pyramid',
      'python-fbthrift',
      'python3-fbthrift',
      'fbthrift-compiler',
      'python-sphinx',
      'python-sphinxcontrib.spelling',
      'python-alabaster',
      'python-posix-ipc',
      'python3-posix-ipc',
      'python-webtest',
      'python3-webtest',
      'python-coverage',
      'python3-coverage',
      'python-nose',
      'python3-nose',
      'python-mock',
      'python3-mock',
    ]:
      ensure  => installed,
      require => [
        Exec['reddit-repo-add'], # Add the reddit ppa
        Exec['update-apt'],      # The system update needs to run
      ],
  } ->

  # Set up the app
  exec { 'install-app':
    cwd     => $project_path,
    command => 'make',
  }

}

include baseplate
