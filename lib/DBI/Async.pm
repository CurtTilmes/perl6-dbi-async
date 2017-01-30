use v6;

use DBIish;

class DBI::Async::Results {
    has $.da;
    has $.dbh;
    has $.sth handles <column-names column-types allrows row rows>;

    method array     { LEAVE self.finish; $!sth.row }
    method hash      { LEAVE self.finish; $!sth.row(:hash) }
    method arrays    { LEAVE self.finish; $!sth.allrows.eager }
    method flatarray { LEAVE self.finish; $!sth.allrows.map({ |$_ }).eager }
    method hashes    { LEAVE self.finish; $!sth.allrows(:array-of-hash).eager }

    method finish {
        .finish with $!sth;
        $!da.reuse-handle($!dbh) if $!da && $!dbh;
        $!da = $!dbh = $!sth = Nil;
    }

    method DESTROY {
        self.finish;
    }
}

my %prepare-cache;
my $cache-lock = Lock.new;

class DBI::Async {
    has Int $!connections;
    has Capture $!dbi-args;
    has Channel $!queries = Channel.new;
    has Channel $!handles = Channel.new;

    method new(:$connections = 5, |args) {
        my $self = self.bless(:$connections, dbi-args => args);
        start $self.process;
        return $self;
    }

    submethod BUILD(:$!connections, :$!dbi-args) {}

    method process() {
        react {
            whenever $!queries {
                my ($promise, $query, @params) = $_;

                my $dbh = self.get-handle();

                start {
                    try {
                        $promise.keep(self.perform($dbh, $query, |@params));
                        CATCH {
                            self.reuse-handle($dbh);
                            $promise.break($_);
                        }
                    }
                }
            }
        }
    }

    method check-handle($dbh) returns Bool {
        if !$dbh.can('ping') or $dbh.ping {
            return True;
        }
        $dbh.dispose;
        $cache-lock.protect({ %prepare-cache{$dbh}:delete });
        $!connections++;
        return False;
    }

    method get-handle() {
        loop {

            # Handle already queued and ready for use

            while $!handles.poll -> $dbh {
                return $dbh if self.check-handle($dbh);
            }

            # Can we make another handle?

            if $!connections > 0 {
                $!connections--;

                my $tries = 1;
                loop {
                    try {
                        return DBIish.connect(|$!dbi-args);

                        CATCH {
                            when X::DBDish::ConnectionFailed {
                                $*ERR.print: "$tries: $_.native-message()";
                            }
                        }
                    }
                    sleep $tries++;
                    $tries min= 60;
                }
            }

            # Wait for a handle in use to become available
        
            with $!handles.receive -> $dbh {
                return $dbh if self.check-handle($dbh);
            }
        }
    }

    method reuse-handle($dbh) {
        $!handles.send($dbh);
    }

    method dispose() {
        while $!handles.poll -> $dbh {
            $dbh.dispose;
        }
    }

    method DESTROY() {
        self.dispose;
    }

    method query($query, *@params, Bool :$async) {
        my $p = Promise.new;
        $!queries.send: ($p, $query, |@params);
        $async ?? $p !! $p.result;
    }

    method perform($dbh, $query, *@params) {
        my $sth;

        try {
            $cache-lock.protect({
                $sth = %prepare-cache{$dbh}{$query} //
                      (%prepare-cache{$dbh}{$query} = $dbh.prepare($query));
            });

            $sth.execute(|@params);

            CATCH {
                .finish with $sth;
            }
        }

        DBI::Async::Results.new(da => self, :$dbh, :$sth);
    }
}
