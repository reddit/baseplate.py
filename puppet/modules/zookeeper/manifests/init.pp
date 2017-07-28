class zookeeper {
  package { 'zookeeperd':
    ensure => installed,
  }
}
