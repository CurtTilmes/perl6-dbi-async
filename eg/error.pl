#!/usr/bin/env perl6

use Log::Async;
use Log::Async::CommandLine;

use DBI::Async;

{
    my $db = DBI::Async.new('Pg');

    my $res will leave { .finish } = $db.query("select version()");

    say $res.row[0];
}

say "done";



