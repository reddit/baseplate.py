class vault {
  package { 'unzip':
    ensure => installed,
  }

  user { 'vault':
    ensure => present,
    system => true,
  }

  exec { 'download vault zipfile':
    command => '/usr/bin/curl -o /var/cache/vault.zip https://releases.hashicorp.com/vault/0.7.3/vault_0.7.3_linux_amd64.zip',
    creates => '/var/cache/vault.zip',
  }

  exec { 'verify zipfile':
    command => '/usr/bin/sha256sum /var/cache/vault.zip | /bin/grep 2822164d5dd347debae8b3370f73f9564a037fc18e9adcabca5907201e5aab45',
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
