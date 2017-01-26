use v6;

use DBIish;
use DBI::Async::Results;

unit class DBI::Async;

my %prepare-cache;
my $cache-lock = Lock.new;

has Capture $!dbi-args;
has Channel $!handles;
has Channel $!queries = Channel.new;

has $!working = False;
has $!lock = Lock.new;

method new(:$connections = 5, |args)
{
    my $handles = Channel.new;
    for 0 ..^ $connections
    {
        $handles.send: DBIish.connect(|args);
    }
    self.bless(:$handles, dbi-args => args);
}

method BUILD(:$!handles, :$!dbi-args) {}

method query($query, *@params, Bool :$async)
{
    if $!handles.poll -> $dbh
    {
        return $async
            ?? start { self!perform($dbh, $query, |@params) }
            !! self!perform($dbh, $query, |@params);
    }

    my $p = Promise.new;

    $!queries.send: ($p, $query, |@params);

    self!process unless $!working;

    $async ?? $p !! $p.result;
}

method !perform($dbh, $query, *@params)
{
    my $sth;

    try
    {
        $cache-lock.protect({
            $sth = %prepare-cache{$dbh}{$query} //
                  (%prepare-cache{$dbh}{$query} = $dbh.prepare($query));
        });

        $sth.execute(|@params);

        CATCH
        {
            .finish with $sth;
            self.reuse($dbh);
            .throw;
        }
    }

    DBI::Async::Results.new(da => self, :$dbh, :$sth);
}

method !process()
{
    $!lock.protect({
        return if $!working;
        $!working = True;
    });

    start loop
    {
        my ($promise, $query, @params) = $!queries.poll;
        
        last unless $promise.defined;
        
        my $dbh = $!handles.receive;
        
        start { $promise.keep(self!perform($dbh, $query, |@params)) }

        LAST $!lock.protect({ $!working = False });
    }
}

method reuse($dbh)
{
    $!handles.send($dbh);
}
