use v6;

use DBIish;

class DBI::Async::Results {
    has $.da;
    has $.dbh;
    has $.sth handles <column-names column-types allrows row rows>;

    method array { LEAVE self.finish; $!sth.row }
    method hash { LEAVE self.finish; $!sth.row(:hash) }
    method arrays { LEAVE self.finish; $!sth.allrows.eager }
    method flatarray { LEAVE self.finish; $!sth.allrows.map({ |$_ }).eager }
    method hashes { LEAVE self.finish; $!sth.allrows(:array-of-hash).eager }

    method finish {
        .finish with $!sth;
        $!da.reuse($!dbh) if $!da && $!dbh;
        $!da = $!dbh = $!sth = Nil;
    }

    method DESTROY {
        self.finish;
    }
}

my %prepare-cache;
my $cache-lock = Lock.new;

class DBI::Async {
    has Capture $!dbi-args;
    has Channel $!handles;
    has Channel $!queries = Channel.new;
    has $!working = False;
    has $!lock = Lock.new;

    method new(:$connections = 5, |args) {
        my $handles = Channel.new;
        for 0 ..^ $connections {
            $handles.send: DBIish.connect(|args);
        }
        self.bless(:$handles, dbi-args => args);
    }

    method BUILD(:$!handles, :$!dbi-args) {}

    method query($query, *@params, Bool :$async) {
        if $!handles.poll -> $dbh {
            return $async
                ?? start { self!perform($dbh, $query, |@params) }
                !! self!perform($dbh, $query, |@params);
        }

        my $p = Promise.new;

        $!queries.send: ($p, $query, |@params);

        self!process unless $!working;

        $async ?? $p !! $p.result;
    }

    method !perform($dbh, $query, *@params) {
        my $sth;

        try {
            $cache-lock.protect({
                $sth = %prepare-cache{$dbh}{$query} //
                    (%prepare-cache{$dbh}{$query} = $dbh.prepare($query));
            });

            $sth.execute(|@params);

            CATCH {
                .finish with $sth;
                self.reuse($dbh);
                .throw;
            }
        }

        DBI::Async::Results.new(da => self, :$dbh, :$sth);
    }

    method !process() {
        $!lock.protect({
            return if $!working;
            $!working = True;
        });

        start loop {
            my ($promise, $query, @params) = $!queries.poll;
        
            last unless $promise.defined;
        
            my $dbh = $!handles.receive;
        
            start { $promise.keep(self!perform($dbh, $query, |@params)) }

            LAST $!lock.protect({ $!working = False });
        }
    }

    method reuse($dbh) {
        $!handles.send($dbh);
    }

    method dispose() {
        while $!handles.poll -> $dbh {
            $dbh.dispose;
        }
    }

    method DESTROY()
    {
        self.dispose;
    }
}
