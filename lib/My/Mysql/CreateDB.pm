package My::Mysql::CreateDB;

use strict;
use warnings;
use 5.019;
use Carp;
use Data::Dump qw(dump);
use Readonly;
use DBIx::Connector;
use DBD::mysql;
use IPC::ShellCmd;
use Memoize;
use autodie qw( :all );
use Moose;
use MooseX::Types::Path::Class;

with 'MooseX::SimpleConfig';
with 'MooseX::Getopt';

memoize 'get_databases';

Readonly my $MYSQL     => q(/usr/bin/mysql);

has 'user' => (
    is  => 'ro',
    isa => 'Str',
);

has 'password' => (
    is  => 'ro',
    isa => 'Str',
);

has 'database' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has 'db_conn' => (
    is         => 'ro',
    isa        => 'DBIx::Connector',
    lazy_build => 1,
);

has 'dry_run' => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0,
);

sub _build_db_conn {
    my ($self) = @_;

    return DBIx::Connector->new(
        "DBI:mysql:database=test",
        $self->user,
        $self->password,
        {
            RaiseError => 1,
            AutoCommit => 1
        }
    );
}

sub _get_cli {
    my ($self) = @_;

    my @cli = $MYSQL;
    push @cli, '-u' . $self->user()     if defined $self->user;
    push @cli, '-p' . $self->password() if defined $self->password;

    return @cli;
}

sub get_databases {
    my ($self) = @_;

    say 'Querying Databases';
    my $sth = $self->db_conn->run(
        fixup => sub {
            my $sth = $_->prepare('SHOW DATABASES');
            $sth->execute;
            $sth;
        }
    );

    #array of array ref
    my @dbs = map { @$_ } map { @$_ } $sth->fetchall_arrayref;
    return \@dbs;
}

sub _database_exist {
    my ( $self, $db ) = @_;

    return grep { $_ eq $self->database } @{ $self->get_databases() };
}

sub _get_create_database_command {
    my ($self) = @_;

    my @cmd     = $self->_get_cli();
    confess "Database " . $self->database . " already exists" if $self->_database_exist();
    say dump(@cmd);

    return @cmd;
}

sub create_databases {
    my ($self) = @_;

    my @cmd = $self->_get_create_database_command();
    say dump(@cmd);
    my $ipc = IPC::ShellCmd->new( \@cmd );

    my $create_ddl = q(CREATE DATABASE ) . $self->database;
    $ipc->stdin($create_ddl);
    $ipc->run();
    say $ipc->stderr();
    say $ipc->stdout();
    croak $ipc->stderr() . $ipc->stdout()  if $ipc->status();
    return;
}

my $app = My::Mysql::CreateDB->new_with_options();
$app->create_databases;
