#!/usr/bin/env perl6

use Log::Async;
use Log::Async::CommandLine;

use DBI::Async;

my $db = DBI::Async.new('Pg');

$db.query("select version() || pg_sleep(1)", :async).then(-> $p {
    say $p.result.array[0];
});

sleep 2;

say "done";



