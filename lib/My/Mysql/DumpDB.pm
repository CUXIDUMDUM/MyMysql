package My::Mysql::DumpDB;

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
Readonly my $MYSQLDUMP => q(/usr/bin/mysqldump);

has 'user' => (
    is  => 'ro',
    isa => 'Str',
);

has 'password' => (
    is  => 'ro',
    isa => 'Str',
);

has 'databases' => (
    is       => 'ro',
    isa      => 'ArrayRef[Str]',
    required => 1,
);

has 'dump_file' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    coerce   => 1,
    required => 1,
    default  => q(/tmp/backup),
);

has 'db_conn' => (
    is         => 'ro',
    isa        => 'DBIx::Connector',
    lazy_build => 1,
);

has 'mysqldump_options' => (
    is      => 'ro',
    isa     => 'ArrayRef[Str]',
    default => sub { [] },
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

    my @cli = $MYSQLDUMP;
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

sub _is_valid_database {
    my ( $self, $db ) = @_;

    return grep { $_ eq $db } @{ $self->get_databases() };
}

sub _is_invalid_database {
    my ( $self, $db ) = @_;

    return not $self->_is_valid_database($db);
}

sub _get_dump_database_command {
    my ($self) = @_;

    my @all_dbs = @{ $self->get_databases() };
    my @cmd     = $self->_get_cli();
    push( @cmd, @{ $self->mysqldump_options } );
    push( @cmd, '--databases' );
    for my $db ( @{ $self->databases } ) {
        push( @cmd, $db );
        confess "Invalid database $db" if $self->_is_invalid_database($db);
    }
    say dump(@cmd);

    return @cmd;
}

sub dump_databases {
    my ($self) = @_;

    my @cmd = $self->_get_dump_database_command();
    say dump(@cmd);
    my $ipc = IPC::ShellCmd->new( \@cmd );

    $ipc->stdout( '-filename' => $self->dump_file->stringify );
    $ipc->run();
    say $ipc->stderr();
    croak $ipc->stderr() if $ipc->status();
}

my $app = My::Mysql::DumpDB->new_with_options();
$app->dump_databases;
