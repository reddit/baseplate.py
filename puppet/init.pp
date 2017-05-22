Exec { path => [ '/usr/bin', '/usr/sbin', '/bin', '/usr/local/bin' ] }

exec { 'update apt cache':
  command     => 'apt-get update',
  refreshonly => true,
}

# this makes the VM available as baseplate.local via mDNS
package { 'avahi-daemon':
  ensure => installed,
}

# make updating the apt cache an implicit requirement for all packages
Exec['update apt cache'] -> Package<| |>

# the dev environment
include baseplate

# for viewing generated docs and reports
include nginx

# for integration tests
include cassandra
include memcached
include redis
include vault
