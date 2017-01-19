use v6;

unit class DBI::Async::Results;

has $.da;
has $.dbh;
has $.sth handles <column-names column-types allrows row rows>;

method array { $!sth.row }

method columns { $!sth.column-names }

method hash { $!sth.row(:hash) }

method arrays { $!sth.allrows }

method hashes { $!sth.allrows(:array-of-hash) }

method finish
{
    $!sth.finish if $!sth;
    $!da.reuse($!dbh) if $!da && $!dbh;
    $!da = $!dbh = $!sth = Nil;
}

method DESTROY
{
    self.finish;
}
