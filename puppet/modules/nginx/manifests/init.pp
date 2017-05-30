# this installs a simple nginx that serves the build directory so that you can
# view generated docs and coverage reports.
class nginx {
  package { 'nginx':
    ensure => installed,
  }

  file { '/etc/nginx/sites-enabled/default':
    ensure  => absent,
    require => Package['nginx'],
    notify  => Service['nginx'],
  }

  file { '/etc/nginx/sites-available/baseplate':
    ensure  => file,
    content => template('nginx/baseplate.conf.erb'),
    owner   => 'root',
    group   => 'root',
    mode    => '0644',
    require => Package['nginx'],
    notify  => Service['nginx'],
  }

  file { '/etc/nginx/sites-enabled/baseplate':
    ensure  => link,
    target  => '/etc/nginx/sites-available/baseplate',
    require => Package['nginx'],
    notify  => Service['nginx'],
  }

  service { 'nginx':
    ensure => running,
  }
}
