Summary: A Tool for Prefetching MySQL Slave Relay Logs, to make SQL Thread faster
Name: replication-booster
Version: 0.2
Release: 1%{?dist}
License: GPLv2+
Group: Applications/System
Source: %{name}-%{version}.tar.gz
URL: https://github.com/yoshinorim/replication-booster-for-mysql
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: gcc-c++, cmake, boost-devel
Requires: boost, boost-thread, boost-regex
Requires: mysql-replication-listener

%description
Replication Booster -- A Tool for Prefetching MySQL Slave Relay Logs,
to make SQL Thread faster.

%prep
%setup -q

%build
%cmake .
make VERBOSE=1

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, root, root)
%{_bindir}/replication_booster

%changelog
* Mon Apr 02 2012 Davi Arnaut <davi@twitter.com>
  Requires mysql-replication-listener
