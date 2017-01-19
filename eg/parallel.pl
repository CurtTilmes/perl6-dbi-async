#!/usr/bin/env perl6

use DBI::Async;

my $db = DBI::Async.new('Pg', connections => 5);

my @list;

for 1..100
{
    @list.push(start {
        say "starting $_";

        say "Done #", $db.query("select pg_sleep(1)::text, ?::int as val",
                                $_).array[1];
    });
}

await @list;

say "done";
