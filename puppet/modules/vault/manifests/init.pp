class vault {
  package { 'unzip':
    ensure => installed,
  }

  user { 'vault':
    ensure => present,
    system => true,
  }

  exec { 'download vault zipfile':
    command => '/usr/bin/curl -o /var/cache/vault.zip https://releases.hashicorp.com/vault/0.6.2/vault_0.6.2_linux_amd64.zip',
    creates => '/var/cache/vault.zip',
  }

  exec { 'verify zipfile':
    command => '/usr/bin/sha256sum /var/cache/vault.zip | /bin/grep 91432c812b1264306f8d1ecf7dd237c3d7a8b2b6aebf4f887e487c4e7f69338c',
    require => Exec['download vault zipfile'],
  }

  exec { 'decompress vault binary':
    command => '/usr/bin/unzip -d /usr/local/bin/ /var/cache/vault.zip',
    creates => '/usr/local/bin/vault',
    cwd     => '/var/cache/',
    require => [
      Exec['verify zipfile'],
      Package['unzip'],
    ]
  }

  file { '/etc/init/vault.conf':
    ensure  => file,
    source  => 'puppet:///modules/vault/vault.conf',
    owner   => 'root',
    group   => 'root',
    mode    => '0644',
    require => [
      Exec['decompress vault binary'],
      User['vault'],
    ]
  }

  service { 'vault':
    ensure  => running,
    require => File['/etc/init/vault.conf'],
  }

  file { '/etc/profile.d/vault.sh':
    ensure => file,
    source => 'puppet:///modules/vault/vault.sh',
    owner  => 'root',
    group  => 'root',
    mode   => '0644',
  }
}
