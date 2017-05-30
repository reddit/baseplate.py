class memcached {
  package { 'memcached':
    ensure => installed,
  }
}
