package My::Mysql::RunDDL;

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
use Try::Tiny;

use Moose;
use MooseX::Types::Path::Class;

with 'MooseX::SimpleConfig';
with 'MooseX::Getopt';


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
    is  => 'ro',
    isa => 'Str',
    default => 'test',
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

has 'schema_file' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    coerce   => 1,
    required => 1,
);

sub _build_db_conn {
    my ($self) = @_;

    return DBIx::Connector->new(
        "DBI:mysql:database=" . $self->database,
        $self->user,
        $self->password,
        {
            RaiseError => 1,
            AutoCommit => 1,
        }
    );
}

sub run_ddl {
    my ($self) = @_;

    my $ddl = $self->schema_file->slurp;

    my $error = undef;
    $self->db_conn->run(
        ping => sub {
            try {
                $_->do($ddl);
            } catch { $error = $_ };
        }
    );

    croak "DDL Failed. Errors " . $error if $error;
    return;
}

my $app = My::Mysql::RunDDL->new_with_options();
$app->run_ddl();
