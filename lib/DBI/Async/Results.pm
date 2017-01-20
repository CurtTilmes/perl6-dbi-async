use v6;

unit class DBI::Async::Results;

has $.da;
has $.dbh;
has $.sth handles <column-names column-types allrows row rows>;

method array { LEAVE self.finish; $!sth.row }

method hash { LEAVE self.finish; $!sth.row(:hash) }

method arrays { LEAVE self.finish; $!sth.allrows.eager }

method flatarray { LEAVE self.finish; $!sth.allrows.map({ |$_ }).eager }

method hashes { LEAVE self.finish; $!sth.allrows(:array-of-hash).eager }

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
